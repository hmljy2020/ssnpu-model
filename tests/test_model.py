import copy
import json
import tempfile
import unittest
from collections import Counter, defaultdict
from pathlib import Path

from api import ModelError, default_config, simulate
from config import validate_config, wse_config
from frontend.capture import TraceAdapter
from simulator import Simulator

ROOT = Path(__file__).resolve().parents[1]
WORK = ROOT / 'workspace/ssnpu-components/tests'


def tile(oid, producer, shape=(1, 4), version=1, shared=False, handle=None):
    o = dict(object_id=oid, storage_id=oid, version=version, kind='tile',
             shape=list(shape), valid_shape=list(shape), dtype='float32',
             producer=producer, memory_space='SharedTileReg' if shared else 'LocalTileReg')
    if shared:
        o.update(shared_version=f'v{oid}', shared_handle=handle or f'h{oid}')
    return o


def op(oid, pe=0, inputs=(), outputs=None, name='tile.full', phase=0, deps=()):
    return dict(op_id=oid, task_id=0, block_id=0, subblock_id=0, thread_id=pe,
                ir_op=name, parameters={}, inputs=list(inputs),
                outputs=[tile(oid, oid)] if outputs is None else outputs,
                dependencies=list(deps), sync_dependencies=[], memory_accesses=[],
                ssnpu=dict(phase=phase, participants=[pe], shared_inputs=[]))


def fixture(path, rows, barriers=(), threads=None):
    path.mkdir(parents=True, exist_ok=True)
    threads = sorted({r['thread_id'] for r in rows}) if threads is None else threads
    instances = []
    for pe in threads:
        rr = [copy.deepcopy(r) for r in rows if r['thread_id'] == pe]
        previous = None
        for sequence, r in enumerate(rr):
            r['instance_op_sequence'] = sequence
            r['order_dependencies'] = [] if previous is None else [previous]
            previous = r['op_id']
        filename = f'thread_{pe}.jsonl'
        (path / filename).write_text(''.join(json.dumps(r) + '\n' for r in rr))
        instances.append(dict(task_id=0, block_id=0, subblock_id=0, thread_id=pe,
                              mapping=dict(core_id=0, pe_id=pe), operation_count=len(rr),
                              status='completed', trace_file=filename))
    events = []
    for round_ in barriers:
        prior = [r['op_id'] for r in rows if r['ssnpu']['phase'] <= round_]
        for pe in threads:
            events.append(dict(event_id=len(events), kind='ssnpu.barrier', task_id=0,
                               block_id=0, thread_id=pe, completion_dependencies=prior,
                               parameters=dict(round=round_, barrier_id=f'b{round_}', participants=threads)))
    (path / 'events.jsonl').write_text(''.join(json.dumps(e) + '\n' for e in events))
    (path / 'tasks.json').write_text(json.dumps([dict(task_id=0, dependencies=[], status='completed')]))
    (path / 'objects.json').write_text('[]')
    (path / 'manifest.json').write_text(json.dumps(dict(format_version=2, ssnpu_spec_version=1,
        complete=True, instances=instances, objects_file='objects.json', tasks_file='tasks.json', events_file='events.jsonl')))
    return path


def audit(report, trace):
    """Independent invariant checks against exported times and capture edges."""
    rows = {r['op_id']: r for r in report['timeline']}
    assert len(rows) == len(report['timeline'])
    assert rows.keys() == trace.instructions.keys()
    pe_issues, core_issues = Counter(), Counter()
    resources = defaultdict(list)
    for key, r in rows.items():
        i = trace.instructions[key]
        if not i.control:
            assert r['admitted'] + 1 == r['admission_received'] <= r['issue']
            if not i.alias:
                assert r['admission_received'] <= r['allocation_request']
        else:
            assert 'admitted' not in r and 'admission_received' not in r
        assert all(rows[d]['complete'] + 2 <= r['issue'] for d in i.deps | i.sync)
        assert r['complete'] >= r['issue']
        assert r['complete'] + 1 == r['rob_complete'] <= r['retire'] <= report['total_cycles']
        assert r['state'] == 'retired'
        if not (i.alias or i.control):
            pe_issues[r['core'], r['pe'], r['issue']] += 1
            core_issues[r['core'], r['issue']] += 1
        prior = r['issue']
        for stage in r['stages']:
            assert prior <= stage['start'] < stage['service_end'] <= stage['complete']
            resources[stage['resource']].append((stage['start'], stage['service_end']))
            prior = stage['complete']
        if r['stages']:
            assert prior <= r['complete']
        if not (i.alias or i.control):
            assert r['issue'] < r['received'] <= r['operands_ready'] <= r['compute_start']
            assert r['compute_start'] < r['compute_end'] <= r['writeback_start'] <= r['writeback_end'] == r['complete']
            reads = [a for a in r['accesses'] if a['kind'] == 'read']
            writes = [a for a in r['accesses'] if a['kind'] == 'write']
            assert len(reads) == len(i.operands)
            assert len(writes) == len(i.outputs)
            assert all(a['complete'] >= a['start'] + 2 for a in r['accesses'])
            assert all(a['complete'] <= b['start'] for a, b in zip(reads, reads[1:]))
            assert all(a['complete'] <= r['compute_start'] for a in reads)
            assert all(a['start'] >= r['compute_end'] for a in writes)
    c = report['config']['parameters']
    assert max(pe_issues.values(), default=0) <= c['pe_issue_width']['value']
    assert max(core_issues.values(), default=0) <= c['core_issue_width']['value']
    for intervals in resources.values():
        ordered = sorted(intervals)
        assert all(a[1] <= b[0] for a, b in zip(ordered, ordered[1:]))
    usage, peaks = Counter(), Counter()
    live, allocated, freed = set(), set(), set()
    for e in report['storage_events']:
        version = trace.versions[tuple(e['version'])]
        assert version.size == e['bytes']
        if e['action'] == 'free':
            key = tuple(e['version'])
            assert key in live and key not in freed
            live.remove(key)
            freed.add(key)
            assert not version.retained
            assert rows[version.producer]['complete'] <= e['cycle']
            for reader in version.consumers:
                assert rows[reader]['complete'] <= e['cycle']
        else:
            key = tuple(e['version'])
            assert key not in allocated
            allocated.add(key)
            live.add(key)
        usage[e['pool']] += e['bytes'] * (1 if e['action'] == 'allocate' else -1)
        peaks[e['pool']] = max(peaks[e['pool']], usage[e['pool']])
        assert 0 <= usage[e['pool']] <= c['shared_capacity' if e['pool'].endswith('/shared') else 'local_capacity']['value']
    assert dict(peaks) == report['storage_peak_bytes']
    assert allocated == trace.versions.keys()
    assert live == {key for key, v in trace.versions.items() if v.retained}
    assert dict(usage) == report['storage_live_bytes']
    assert report['total_cycles'] >= max((r['retire'] for r in rows.values()), default=0)
    assert report['last_instruction_complete_cycle'] == max((rows[k]['complete'] for k, i in trace.instructions.items() if not i.control), default=0)
    threads = defaultdict(list)
    for i in trace.instructions.values():
        if not i.control:
            threads[i.raw['_identity']].append(i)
    for thread in threads.values():
        ordered = sorted(thread, key=lambda i: i.age)
        assert all(rows[a.id]['retire'] <= rows[b.id]['retire'] for a, b in zip(ordered, ordered[1:]))
        assert all(rows[a.id]['admitted'] <= rows[b.id]['admitted'] for a, b in zip(ordered, ordered[1:]))
        reservations = [i for i in ordered if not i.alias]
        assert all(rows[a.id]['allocation_request'] <= rows[b.id]['allocation_request']
                   for a, b in zip(reservations, reservations[1:]))
        assert all(rows[a.id]['allocated'] <= rows[b.id]['allocated']
                   for a, b in zip(reservations, reservations[1:]))
        for i in reservations:
            assert all(rows[d]['complete'] + 2 <= rows[i.id]['allocation_request'] for d in i.sync)
        changes, admissions = Counter(), Counter()
        for i in ordered:
            changes[rows[i.id]['admitted']] += 1
            admissions[rows[i.id]['admitted']] += 1
            changes[rows[i.id]['retire']] -= 1
        occupancy = peak = full_cycles = admitted = 0
        depth = c.get('rob_depth', {}).get('value')
        cycles = sorted(changes)
        for index, cycle in enumerate(cycles):
            occupancy += changes[cycle]
            admitted += admissions[cycle]
            peak = max(peak, occupancy)
            assert occupancy >= 0 and (depth is None or occupancy <= depth)
            if depth is not None and occupancy == depth and admitted < len(ordered):
                full_cycles += cycles[index+1] - cycle
        expected = dict(capacity=depth, occupancy=0, peak=peak, full_cycles=full_cycles)
        assert report['rob_windows'][f'core:{ordered[0].core}/rob'][str(ordered[0].raw['_identity'])] == expected


class ModelTestCase(unittest.TestCase):
    def setUp(self):
        WORK.mkdir(parents=True, exist_ok=True)
        self.tmp = tempfile.TemporaryDirectory(dir=WORK)
        self.path = Path(self.tmp.name)
        self.c = default_config()
        for e in self.c['engines'].values():
            e.update(throughput=4, latency=2)

    def tearDown(self):
        self.tmp.cleanup()

    def run_rows(self, rows, barriers=(), threads=None):
        fixture(self.path, rows, barriers, threads)
        report = simulate(self.path, self.c)
        audit(report, TraceAdapter(self.path, self.c))
        return report, {r['op_id']: r for r in report['timeline']}

    def shared_rows(self, retention='last-use'):
        source = op(0, outputs=[tile(0, 0, shared=True)])
        readers = [op(p+1, pe=p, inputs=source['outputs'], name='tile.add') for p in range(4)]
        for r in readers:
            r['ssnpu']['shared_inputs'] = [dict(use_id='read', operand=0, version_id='v0',
                handle_id='h0', participants=[0, 1, 2, 3], retention=retention)]
        return [source] + readers


class ModelTests(ModelTestCase):
    def test_throughput_and_latency(self):
        a = op(0)
        b = op(1)
        d = op(2, inputs=a['outputs'], name='tile.add')
        _, t = self.run_rows([a, b, d])
        # Admit 0->1, allocate 1->2->3, dispatch 3->4, compute [4,5)+2, write 7->8->9.
        self.assertEqual([t[k]['issue'] for k in (0, 1)], [3, 3])
        self.assertLess(t[2]['allocated'], t[0]['complete'])
        self.assertEqual(t[2]['issue'], t[0]['complete'] + 2)
        self.assertEqual(t[2]['compute_start'], t[2]['issue'] + 3)
        self.assertEqual(t[2]['complete'], t[2]['compute_start'] + 5)

    def test_ooo_bypass(self):
        slow = op(0, outputs=[tile(0, 0, (1, 40))])
        blocked = op(1, inputs=slow['outputs'], name='tile.exp')
        ready = op(2)
        _, t = self.run_rows([slow, blocked, ready])
        self.assertEqual(t[2]['issue'], 3)
        self.assertEqual(t[2]['compute_start'], 14)
        self.assertEqual(t[1]['issue'], t[0]['complete'] + 2)

    def test_width_and_determinism(self):
        rows = [op(p, pe=p) for p in range(4)]
        self.c['parameters']['core_issue_width']['value'] = 2
        r, t = self.run_rows(rows)
        self.assertEqual([t[p]['issue'] for p in range(4)], [3, 4, 4, 3])
        self.assertEqual(r, simulate(self.path, self.c))
        self.c['parameters']['core_issue_width']['value'] = 4
        _, t = self.run_rows(rows)
        self.assertTrue(all(t[p]['issue'] == 3 for p in range(4)))

    def test_per_pe_issue_width_with_two_engines(self):
        tensor = dict(object_id=99, storage_id=99, version=0, producer=None,
                      shape=[1, 4], dtype='float32', kind='tensor', memory_space='DDR')
        load = op(1, inputs=[tensor], name='tile.load')
        load['memory_accesses'] = [dict(object_id=99, shape=[1, 4])]
        self.c['parameters']['pe_issue_width']['value'] = 1
        _, t = self.run_rows([op(0), load])
        self.assertEqual(t[1]['issue'], 4)
        self.c['parameters']['pe_issue_width']['value'] = 2
        _, t = self.run_rows([op(0), load])
        self.assertEqual(t[1]['issue'], 3)

    def test_shared_memory_contention_and_four_pe_compute(self):
        rows = []
        for p in range(4):
            tensor = dict(object_id=90, storage_id=90, version=0, producer=None,
                          shape=[1, 4], dtype='float32', kind='tensor', memory_space='DDR')
            load = op(10+p, pe=p, inputs=[tensor], name='tile.load')
            load['memory_accesses'] = [dict(object_id=90, shape=[1, 4])]
            rows.extend([op(p, pe=p), load])
        _, t = self.run_rows(rows)
        self.assertEqual(sorted(t[10+p]['issue'] for p in range(4)), [4, 4, 4, 4])
        self.assertEqual(sorted(t[10+p]['compute_start'] for p in range(4)), [5, 9, 13, 17])
        self.assertEqual({t[p]['issue'] for p in range(4)}, {3})

    def test_shared_last_use_and_reuse_liveout(self):
        rows = self.shared_rows()
        r, t = self.run_rows(rows)
        frees = [e for e in r['storage_events'] if e['pool'].endswith('/shared') and e['action'] == 'free']
        self.assertEqual(frees[0]['cycle'], max(t[k]['complete'] for k in range(1, 5)) + 1)
        rows = self.shared_rows('reuse')
        r, _ = self.run_rows(rows)
        self.assertEqual(r['storage_live_bytes']['core:0/shared'], 16)

    def test_alias_and_repeated_shared_versions(self):
        rows = self.shared_rows()
        view = op(10, inputs=rows[0]['outputs'], name='tile.transpose_view',
                  outputs=[dict(rows[0]['outputs'][0], object_id=100, shape=[4, 1])])
        rows.insert(1, view)
        rows[2]['inputs'] = [dict(view['outputs'][0], view_producer=10)]
        rows.append(op(11, outputs=[dict(tile(11, 11, shared=True, handle='h0'), storage_id=0)]))
        rows.append(op(12, inputs=rows[-1]['outputs'], name='tile.add'))
        rows[-1]['ssnpu']['shared_inputs'] = [dict(use_id='read2', operand=0, version_id='v11', handle_id='h0', participants=[0], retention='last-use')]
        r, t = self.run_rows(rows)
        self.assertEqual(t[10]['stages'], [])
        self.assertEqual(r['storage_peak_bytes']['core:0/shared'], 32)
        self.assertLess(t[11]['issue'], max(t[k]['complete'] for k in range(1, 5)))

    def test_capacity_wait_and_recovery(self):
        self.c['parameters']['local_capacity']['value'] = 16
        r, t = self.run_rows([op(0), op(1)])
        self.assertEqual(t[1]['issue'], 11)
        self.assertTrue(any('capacity:' in reason for reason in r['wait_reason_sets']['1']))

    def test_local_read_old_write_new_keeps_both_versions(self):
        a = op(0)
        b = op(1, inputs=a['outputs'], name='tile.exp')
        c = op(2, inputs=a['outputs'], outputs=[tile(0, 2, version=2)], name='tile.add')
        self.c['engines']['exp']['latency'] = 100
        report, t = self.run_rows([a, b, c])
        self.assertLess(t[2]['issue'], t[1]['complete'])
        self.assertEqual(t[2]['issue'], t[0]['complete'] + 2)
        self.assertEqual(report['storage_peak_bytes']['core:0/pe:0/local'], 48)
        self.c['parameters']['local_capacity']['value'] = 16
        with self.assertRaisesRegex(ModelError, 'Deadlock.*capacity:'):
            self.run_rows([a, c])
        self.c['parameters']['local_capacity']['value'] = 32
        report, _ = self.run_rows([a, c])
        self.assertEqual(report['storage_peak_bytes']['core:0/pe:0/local'], 32)
        self.assertFalse(any('superseded_by' in e for e in report['storage_events']))

    def test_empty_phases_and_tail_barrier(self):
        self.c['parameters']['barrier_latency']['value'] = 2
        r, t = self.run_rows([op(0), op(1, phase=2)], barriers=[0, 1, 2], threads=[0, 1, 2, 3])
        self.assertEqual(t[1]['issue'], 21)
        self.assertEqual(r['total_cycles'], 35)
        r, _ = self.run_rows([], barriers=[0, 1], threads=[0, 1, 2, 3])
        self.assertEqual(r['total_cycles'], 10)

    def test_errors(self):
        with self.assertRaisesRegex(ModelError, 'Missing producer'):
            self.run_rows([op(0, inputs=[tile(99, None)])])
        with self.assertRaisesRegex(ModelError, 'Missing dependencies'):
            self.run_rows([op(0, deps=[999])])
        with self.assertRaisesRegex(ModelError, 'Unknown operation'):
            self.run_rows([op(0, name='tile.bogus')])
        with self.assertRaisesRegex(ModelError, 'cycle'):
            self.run_rows([op(0, deps=[1]), op(1, deps=[0])])
        self.c['parameters']['local_capacity']['value'] = 1
        with self.assertRaisesRegex(ModelError, 'Deadlock.*capacity:'):
            self.run_rows([op(0)])

    def test_bad_participants_and_lifetime(self):
        rows = self.shared_rows()
        rows.pop()
        with self.assertRaisesRegex(ModelError, 'participant mismatch'):
            self.run_rows(rows)
        rows = self.shared_rows()
        extra = copy.deepcopy(rows[1])
        extra['op_id'] = 10
        extra['outputs'] = [tile(10, 10)]
        extra['ssnpu']['shared_inputs'][0].update(use_id='late', participants=[0], retention='reuse')
        with self.assertRaisesRegex(ModelError, 'Read after last-use'):
            self.run_rows(rows + [extra])

    def test_task_dependency(self):
        fixture(self.path, [op(0)])
        tasks = [dict(task_id=0, dependencies=[1], status='completed'),
                 dict(task_id=1, dependencies=[], status='completed')]
        (self.path / 'tasks.json').write_text(json.dumps(tasks))
        r = simulate(self.path, self.c)
        audit(r, TraceAdapter(self.path, self.c))
        t = {row['op_id']: row for row in r['timeline']}
        self.assertEqual(t['task:1']['issue'], 0)
        self.assertEqual(t[0]['allocation_request'], t['task:1']['complete'] + 2)

    def test_dtype_and_zero_service_work(self):
        self.c['engines']['vector']['dtype_overrides']['float32'] = dict(throughput=1, latency=0)
        _, t = self.run_rows([op(0)])
        self.assertEqual(t[0]['complete'], 10)
        self.c['engines']['vector']['dtype_overrides']['float32']['throughput'] = 0
        with self.assertRaisesRegex(ModelError, 'throughput'):
            simulate(self.path, self.c)

class AdditionalTests(ModelTestCase):
    def test_shared_handle_independent_physical_versions(self):
        rows = self.shared_rows()
        new = op(11, outputs=[tile(11, 11, shared=True, handle='h0')])
        rows.append(new)
        reader = op(12, inputs=new['outputs'], name='tile.add')
        reader['ssnpu']['shared_inputs'] = [dict(use_id='read2', operand=0, version_id='v11',
            handle_id='h0', participants=[0], retention='last-use')]
        rows.append(reader)
        r, t = self.run_rows(rows)
        self.assertLess(t[11]['issue'], max(t[k]['complete'] for k in range(1, 5)))
        self.assertEqual(r['storage_peak_bytes']['core:0/shared'], 32)

    def test_skipped_task_preserves_dependencies(self):
        fixture(self.path, [op(0)])
        tasks = [dict(task_id=0, dependencies=[], status='completed'),
                 dict(task_id=1, dependencies=[0], status='skipped')]
        (self.path / 'tasks.json').write_text(json.dumps(tasks))
        r = simulate(self.path, self.c)
        audit(r, TraceAdapter(self.path, self.c))
        t = {row['op_id']: row for row in r['timeline']}
        self.assertEqual(t['task:1']['issue'], t['task:0']['complete'] + 2)

    def test_zero_work_reduction(self):
        a = op(0, outputs=[tile(0, 0, (4, 1))])
        b = op(1, inputs=a['outputs'], name='tile.row_sum')
        _, t = self.run_rows([a, b])
        stage = t[1]['stages'][0]
        self.assertEqual(stage['work'], 0)
        self.assertEqual(stage['service_end'] - stage['start'], 1)

    def test_padded_storage_capacity(self):
        out = tile(0, 0, (8, 8))
        out['valid_shape'] = [1, 1]
        self.c['parameters']['local_capacity']['value'] = 64
        with self.assertRaisesRegex(ModelError, 'Deadlock.*capacity:'):
            self.run_rows([op(0, outputs=[out])])

    def test_cube_work_and_scope(self):
        rows = []
        for p in range(4):
            a = op(10*p, pe=p, outputs=[tile(10*p, 10*p, (2, 3))])
            b = op(10*p+1, pe=p, outputs=[tile(10*p+1, 10*p+1, (3, 5))])
            c = op(10*p+2, pe=p, inputs=a['outputs']+b['outputs'],
                   outputs=[tile(10*p+2, 10*p+2, (2, 5))], name='tile.matmul')
            rows.extend([a, b, c])
        _, t = self.run_rows(rows)
        for p in range(4):
            self.assertLess(t[p*10+2]['allocated'], t[p*10+1]['complete'])
            self.assertEqual(t[p*10+2]['issue'], max(t[p*10]['complete'], t[p*10+1]['complete']) + 2)
        self.assertEqual({t[p*10+2]['stages'][0]['work'] for p in range(4)}, {30})

    def test_memory_path_override(self):
        tensor = dict(object_id=99, storage_id=99, version=0, producer=None,
                      shape=[1, 4], dtype='float32', kind='tensor', memory_space='L2')
        load = op(0, inputs=[tensor], name='tile.load')
        load['memory_accesses'] = [dict(object_id=99, shape=[1, 4])]
        self.c['paths']['L2'] = dict(engine='memory', throughput=8, latency=3)
        _, t = self.run_rows([load])
        self.assertEqual(t[0]['compute_end'] - t[0]['compute_start'], 5)
        self.assertEqual(t[0]['complete'], 11)

    def test_tlsu_multiple_ranges_and_path_overrides_dtype(self):
        tensors = [dict(object_id=oid, storage_id=oid, version=0, producer=None,
                        shape=[1, 4], dtype='float32', kind='tensor', memory_space=space)
                   for oid, space in ((90, 'DDR'), (91, 'L2'))]
        load = op(0, inputs=tensors, name='tile.load')
        load['memory_accesses'] = [dict(object_id=90, shape=[1, 2]), dict(object_id=91, shape=[1, 4])]
        self.c['engines']['memory']['dtype_overrides']['float32'] = dict(throughput=1, latency=9)
        self.c['paths']['L2'] = dict(engine='memory', throughput=8, latency=1)
        report, t = self.run_rows([load])
        self.assertEqual([(s['work'], s['start'], s['service_end'], s['complete']) for s in t[0]['stages']],
                         [(8, 4, 12, 21), (16, 21, 23, 24)])
        self.assertEqual(t[0]['complete'], 26)
        self.assertEqual(report['resources']['core:0/memory']['bytes'], 24)

    def test_tlsu_rejects_missing_or_unresolved_memory_ranges(self):
        tensor = dict(object_id=90, shape=[1, 4], dtype='float32', kind='tensor', memory_space='DDR')
        load = op(0, inputs=[tensor], name='tile.load')
        with self.assertRaisesRegex(ModelError, 'Missing memory access range'):
            self.run_rows([load])
        load['memory_accesses'] = [dict(object_id=90)]
        with self.assertRaisesRegex(ModelError, 'Unresolved memory access range'):
            self.run_rows([load])
        load['memory_accesses'] = [dict(object_id=90, shape=[1, 4])]
        tensor['memory_space'] = 'unknown'
        with self.assertRaisesRegex(ModelError, 'Unknown memory path'):
            self.run_rows([load])


if __name__ == '__main__':
    unittest.main()
