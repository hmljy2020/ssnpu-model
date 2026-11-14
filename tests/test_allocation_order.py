"""Ordered reservations, out-of-order execution and independent Local generations."""
import json

from api import ModelError, default_config
from frontend.capture import TraceAdapter
from models.ssnpu import Scheduler, TileReg
from runtime.protocol import Admitted, Allocate, Allocated, Completed, Consumed, Dispatch
from test_model import ModelTestCase, fixture, op, tile


class AllocationTests(ModelTestCase):
    def test_reserve_before_data_ready_then_execute_out_of_order(self):
        self.c['engines']['exp'].update(throughput=1, latency=100)
        slow = op(0, name='tile.exp')
        waiting = op(1, inputs=slow['outputs'], name='tile.add')
        report, rows = self.run_rows([slow, waiting, op(2)])
        self.assertEqual([rows[k]['allocation_request'] for k in range(3)], [1] * 3)
        self.assertEqual([rows[k]['allocated'] for k in range(3)], [3] * 3)
        self.assertLess(rows[1]['allocated'], rows[0]['complete'])
        self.assertLess(rows[2]['issue'], rows[1]['issue'])
        self.assertLess(rows[2]['complete'], rows[0]['complete'])
        self.assertIn('dependency', report['wait_reason_sets']['1'])
        self.assertNotIn('dependency|issue_width', report['wait_reason_sets']['1'])

    def test_sync_blocks_younger_reservations_but_not_another_thread(self):
        fixture(self.path, [op(0), op(1), op(2, pe=1)])
        trace = TraceAdapter(self.path, self.c)
        trace.instructions[0].sync.add('gate')
        scheduler = Scheduler('core:0/scheduler',
                              [trace.instructions[k] for k in range(3)], self.c)
        for key in range(3):
            scheduler.receive('admission', Admitted(key))
        scheduler.tick(0)
        requests = scheduler.drain_outputs()
        self.assertEqual([m.instruction for _, _, m in requests], [2])
        waiting = scheduler.snapshot()['waiting']
        self.assertEqual(waiting['0']['reasons'], ['synchronization'])
        self.assertEqual(waiting['1']['reasons'], ['waiting_prior_allocation:0'])
        scheduler.receive('allocation', Allocated(2, 'allocate:2'))
        scheduler.tick(1)
        self.assertTrue(any(isinstance(m, Dispatch) and m.instruction == 2
                            for _, _, m in scheduler.drain_outputs()))
        scheduler.receive('completion', Completed('gate'))
        scheduler.tick(2)
        self.assertEqual([m.instruction for _, _, m in scheduler.drain_outputs()], [0, 1])

    def test_blocked_allocation_holds_younger_requests_and_drains_in_one_tick(self):
        self.c['parameters']['local_capacity']['value'] = 48
        # Existing 16 B leaves 32 B: the 48 B pair blocks the younger 16 B output.
        # Another thread on the same PE can still use those 32 B.
        rows = [op(0), op(1, outputs=[tile(10, 1, (1, 8)), tile(11, 1)]),
                op(2), op(3, pe=1)]
        fixture(self.path, rows)
        manifest_path = self.path / 'manifest.json'
        manifest = json.loads(manifest_path.read_text())
        manifest['instances'][1]['mapping']['pe_id'] = 0
        manifest_path.write_text(json.dumps(manifest))
        trace = TraceAdapter(self.path, self.c)
        reg = TileReg('core:0/tile', trace.instructions, trace.versions, self.c)
        for key in range(4):
            reg.receive('allocation', Allocate(key, f'allocate:{key}'))
        reg.tick(0)
        self.assertEqual([m.instruction for _, _, m in reg.drain_outputs()], [0, 3])
        self.assertEqual(reg.blocked['1'], 'capacity:core:0/pe:0/local')
        self.assertEqual(reg.blocked['2'], 'waiting_prior_allocation:1')
        self.assertFalse(trace.instructions[1].outputs & reg.active)
        self.assertEqual(reg.usage['core:0/pe:0/local'], 32)
        for key in (0, 3):
            reg.receive('consumption', Consumed(key))
        reg.tick(1)
        self.assertEqual([m.instruction for _, _, m in reg.drain_outputs()], [1])
        reg.receive('consumption', Consumed(1))
        reg.tick(2)
        self.assertEqual([m.instruction for _, _, m in reg.drain_outputs()], [2])

        # Several requests that fit must all receive replies in the same tick.
        reg = TileReg('core:0/tile', trace.instructions, trace.versions, self.c)
        for key in (0, 2, 3):
            reg.receive('allocation', Allocate(key, f'allocate:{key}'))
        reg.tick(0)
        allocated = [m.instruction for _, _, m in reg.drain_outputs()]
        self.assertEqual(set(allocated), {0, 2, 3})
        self.assertLess(allocated.index(0), allocated.index(2))

    def test_new_local_version_overlaps_old_reader_and_preserves_read_identity(self):
        self.c['engines']['exp'].update(throughput=4, latency=100)
        self.c['message_log'] = True
        source = op(0)
        reader = op(1, inputs=source['outputs'], name='tile.exp')
        newer = op(2, outputs=[tile(0, 2, version=2)], deps=[0])
        report, rows = self.run_rows([source, reader, newer])
        trace = TraceAdapter(self.path, self.c)
        self.assertEqual(trace.instructions[2].deps, {0})
        self.assertEqual(trace.instructions[1].inputs, {('storage', 0, 1)})
        self.assertEqual(trace.instructions[2].outputs, {('storage', 0, 2)})
        self.assertLess(rows[2]['complete'], rows[1]['complete'])
        self.assertLess(rows[1]['compute_start'], rows[2]['complete'])
        self.assertEqual(report['storage_peak_bytes']['core:0/pe:0/local'], 48)
        old_free, = [e for e in report['storage_events']
                     if e['action'] == 'free' and e['version'] == ['storage', 0, 1]]
        self.assertEqual(old_free['cycle'], rows[1]['complete'] + 1)
        reads = [m for m in report['messages']
                 if m['type'] == 'Access' and m['instruction'] == 1 and not m['write']]
        self.assertEqual([tuple(a['version']) for a in reads], [('storage', 0, 1)])

        # With only two slots, the new version waits for the old reader to finish.
        self.c['parameters']['local_capacity']['value'] = 32
        report, rows = self.run_rows([source, reader, newer])
        self.assertGreater(rows[2]['allocated'], rows[1]['complete'])
        self.assertTrue(any('capacity:' in reason for reason in report['wait_reason_sets']['2']))
        newer['dependencies'] = []
        fixture(self.path, [source, reader, newer])
        self.assertEqual(TraceAdapter(self.path, self.c).instructions[2].deps, set())

    def test_local_alias_chain_has_one_allocation_and_waits_for_source(self):
        source = op(0, name='tile.exp')
        view = op(1, inputs=source['outputs'], name='tile.reshape_view',
                  outputs=[tile(10, 1, (2, 2))])
        view2 = op(2, inputs=view['outputs'], name='tile.transpose_view',
                   outputs=[tile(11, 2, (2, 2))])
        reader = op(3, inputs=view2['outputs'], name='tile.add')
        self.c['parameters']['local_capacity']['value'] = 32
        report, rows = self.run_rows([source, view, view2, reader])
        self.assertGreater(rows[1]['issue'], rows[0]['complete'])
        self.assertGreater(rows[2]['issue'], rows[1]['complete'])
        self.assertNotIn('allocation_request', rows[1])
        self.assertNotIn('allocation_request', rows[2])
        allocated = [e['version'] for e in report['storage_events'] if e['action'] == 'allocate']
        self.assertEqual(allocated, [['storage', 0, 1], ['storage', 3, 1]])
        trace = TraceAdapter(self.path, self.c)
        self.assertEqual(trace.instructions[3].inputs, {('storage', 0, 1)})

    def test_issue_6_requires_three_kib_for_old_and_new_b(self):
        a = op(0, outputs=[tile(0, 0, (16, 16))])
        b = op(1, inputs=a['outputs'], outputs=[tile(1, 1, (16, 16))], name='tile.relu')
        c = op(2, outputs=[tile(2, 2, (16, 16))])
        d = op(3, inputs=b['outputs'] + c['outputs'],
               outputs=[tile(1, 3, (16, 16), version=2)], name='tile.add')
        self.c = default_config()
        for depth in (1, 2, 4, None):
            for capacity in (2048, 3072):
                with self.subTest(depth=depth, capacity=capacity):
                    self.c['parameters']['local_capacity']['value'] = capacity
                    self.c['parameters']['rob_depth']['value'] = depth
                    if capacity == 2048:
                        with self.assertRaisesRegex(ModelError, 'Deadlock.*capacity:') as caught:
                            self.run_rows([a, b, c, d])
                        snapshot = json.loads(str(caught.exception).split(': ', 1)[1])
                        reg = snapshot['components']['core:0/tile']
                        self.assertEqual({tuple(k) for k in reg['active']},
                                         {('storage', 1, 1), ('storage', 2, 1)})
                        self.assertEqual(reg['blocked']['3'], 'capacity:core:0/pe:0/local')
                    else:
                        report, _ = self.run_rows([a, b, c, d])
                        self.assertEqual(report['storage_peak_bytes']['core:0/pe:0/local'], 3072)

    def test_larger_rob_does_not_let_young_output_steal_older_capacity(self):
        # A+B fit in 2 KiB; B consumes A before C needs its slot. Store has no
        # tile output, so 2 KiB is sufficient for the entire program.
        a = op(0, outputs=[tile(0, 0, (16, 16))])
        b = op(1, inputs=a['outputs'], outputs=[tile(1, 1, (16, 16))], name='tile.relu')
        c = op(2, outputs=[tile(2, 2, (16, 16))])
        tensor = dict(object_id=99, kind='tensor', shape=[16, 16], dtype='float32', memory_space='DDR')
        store = op(3, inputs=b['outputs'] + c['outputs'], outputs=[tensor], name='tile.store')
        store['memory_accesses'] = [dict(object_id=99, shape=[16, 16])]
        self.c['parameters']['local_capacity']['value'] = 2048
        for depth in (1, 2, 4, 16, None):
            with self.subTest(depth=depth):
                self.c['parameters']['rob_depth']['value'] = depth
                report, rows = self.run_rows([a, b, c, store])
                self.assertLess(rows[1]['allocated'], rows[2]['allocated'])
                self.assertEqual(report['storage_peak_bytes']['core:0/pe:0/local'], 2048)

    def test_barrier_prevents_cross_phase_reservations(self):
        _, rows = self.run_rows([op(0), op(1, phase=1), op(2, phase=1)], barriers=[0])
        for key in (1, 2):
            self.assertGreater(rows[key]['allocation_request'], rows['barrier:0:0:0']['complete'])
