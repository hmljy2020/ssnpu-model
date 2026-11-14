"""Interface contract tests. Small deterministic byte streams isolate protocol.

No numeric tolerances: standalone AllGather must reproduce every source byte.
Integration uses the same 2-node 256-byte workload as test_noc.
"""
import copy
from dataclasses import replace, asdict
import json
import unittest

from examples.communication.driver import build_demo, run_demo
from models.noc.protocol import (
    Accepted, Rejected, Completion, DataCommand, DataReply, request_from_dict)
from models.noc.unit import CommUnit
from errors import ModelError
import test_noc


class CommUnitTests(unittest.TestCase):
    def test_standalone_staggered_peers_independent_work_and_byte_visibility(self):
        report = run_demo()
        self.assertTrue(report['passed'] and report['drained'])
        self.assertTrue(any(e['kind'] == 'backpressure' for e in report['network']['events']))
        for client in report['clients'].values():
            h = {e['event']: e['cycle'] for e in client['history']}
            self.assertLess(h['accepted'], h['source_released'])
            self.assertLess(h['independent_work_start'], h['independent_work_end'])
            self.assertLess(h['wait'], h['completion'])
            self.assertGreaterEqual(h['consume'], h['completion'])
        # Different local completion times in the same collective prove that
        # success is not waiting for a final all-node barrier.
        completions = [e['time_ns'] for e in report['network']['events']
                       if e['kind'] == 'completion' and e['phase'] == 'demo:0']
        self.assertLess(min(completions), max(completions))
        for e in report['network']['events']:
            if e['kind'] != 'completion':
                continue
            required = [r['time_ns'] for r in report['network']['events'] if r['phase'] == e['phase'] and (
                (r['kind'] == 'tx_drained' and r['source'] == e['node']) or
                (r['kind'] == 'receive_published' and r['node'] == e['node']))]
            self.assertEqual(len(required), 2)
            self.assertGreaterEqual(e['time_ns'], max(required))

    def test_requests_and_installation_survive_json_with_two_digit_node_ids(self):
        unit, clients, _ = build_demo(members=(0, 8, 16, 24, 32), rounds=1)
        restored = json.loads(json.dumps(unit.installation))
        other = CommUnit(restored, json.loads(json.dumps(unit.platform)))
        self.assertEqual(other.installation['version'], unit.installation['version'])
        for client in clients.values():
            request = request_from_dict(json.loads(json.dumps(asdict(client.request))))
            self.assertEqual(request, client.request)
            other.submit(request)
        self.assertEqual(len(other.requests), 5)

    def test_bad_version_epoch_route_range_or_duplicate_is_fatal_rejection(self):
        for change in (dict(version='stale'), dict(epoch=99), dict(members=(0, 1)),
                       dict(route_bits=(0,)), dict(operation='reduce')):
            unit, clients, _ = build_demo(rounds=1)
            request = next(iter(clients.values())).request
            unit.submit(replace(request, **change))
            reply, = [m for _, _, m in unit.drain_outputs()]
            self.assertIsInstance(reply, Rejected)
            self.assertFalse(reply.retryable)
            self.assertFalse(unit.requests)
        unit, clients, _ = build_demo(rounds=1)
        request = next(iter(clients.values())).request
        unit.submit(replace(request, source=replace(request.source, offset=1)))
        reply, = [m for _, _, m in unit.drain_outputs()]
        self.assertIsInstance(reply, Rejected)
        unit.submit(request)
        unit.drain_outputs()
        unit.submit(request)
        reply, = [m for _, _, m in unit.drain_outputs()]
        self.assertFalse(reply.retryable)

    def test_busy_rejection_does_not_capture_request_or_retain_its_binding(self):
        unit, clients, _ = build_demo()
        clients = list(clients.values())
        unit.submit(clients[0].request)
        unit.drain_outputs()
        unit.submit(clients[3].request)
        reply, = [m for _, _, m in unit.drain_outputs()]
        self.assertTrue(reply.retryable)
        self.assertNotIn(clients[3].request.token, unit.tokens)
        self.assertEqual(len(unit.requests), 1)

    def test_platform_or_installation_mutation_rejected(self):
        unit, _, _ = build_demo()
        platform = copy.deepcopy(unit.platform)
        platform['network']['link_bytes_per_cycle'] *= 2
        with self.assertRaisesRegex(ModelError, 'platform/config'):
            CommUnit(unit.installation, platform)
        installation = copy.deepcopy(unit.installation)
        installation['calreg']['demo:0']['epoch'] = 10
        with self.assertRaisesRegex(ModelError, 'version'):
            CommUnit(installation, unit.platform)

    def test_fault_quarantines_resources_and_never_reports_success_or_drain(self):
        for event, detail in (('fault', 'stale-generation'), ('published', None), ('written', (0, 0))):
            unit, clients, _ = build_demo(rounds=1)
            request = next(iter(clients.values())).request
            unit.submit(request)
            unit.drain_outputs()
            unit.receive('data', DataReply(request.token, event, detail))
            unit.tick(4)
            messages = [m for _, _, m in unit.drain_outputs()]
            faults = [m for m in messages if isinstance(m, Completion)]
            self.assertEqual(len(faults), 1)
            self.assertEqual(faults[0].status, 'fault')
            self.assertFalse(faults[0].safe_to_release)
            self.assertFalse(unit.drained())
            self.assertIn(request.token, unit.tokens)
            self.assertFalse(unit.advance(5))

    def test_data_owner_rejects_stale_generation(self):
        _, clients, _ = build_demo(rounds=1)
        client = next(iter(clients.values()))
        stale = replace(client.request.source, handle=('source', 0, -1))
        client.receive('data', DataCommand(client.request.token, 'read', stale))
        client.tick(1)
        reply, = [m for _, _, m in client.drain_outputs()]
        self.assertEqual(reply.event, 'fault')
        self.assertFalse(client.source_released)

    def test_late_wait_can_consume_an_already_completed_token(self):
        # Exercise client token storage independently from the timing backend.
        _, clients, _ = build_demo(rounds=1)
        client = next(iter(clients.values()))
        client.independent_cycles = 100
        client.receive('control', Accepted(client.request.token))
        client.tick(1)
        client.published = client.source_released = True
        client.receive('control', Completion(client.request.token, 'success'))
        client.tick(10)
        self.assertFalse(client.consumed)
        client.tick(101)
        self.assertTrue(client.consumed)


class CoreAdapterTests(unittest.TestCase):
    setUp = test_noc.CalendarTests.setUp
    run_model = test_noc.CalendarTests.run_model

    def test_core_contains_only_requests_and_token_waits(self):
        report = self.run_model()
        rows = {row['op_id']: row for row in report['timeline']}
        for node in (0, 1):
            submit, wait = rows[f'comm:pair:{node}:submit'], rows[f'comm:pair:{node}:wait']
            self.assertEqual(submit['complete'], submit['accepted'])
            self.assertLess(submit['complete'], submit['source_read_complete'])
            self.assertLess(submit['source_read_complete'], wait['complete'])
            # Submit reserves the receive before source readiness; wait's own
            # Allocate can now arrive in the same tick and must be a no-op.
            self.assertEqual(submit['allocated'], wait['allocated'])
            self.assertLess(submit['allocated'], submit['complete'])
            entry = next(e for e in self.plan['phases'][0]['nodes'] if e['core'] == node)
            target = ['shared', entry['target']['shared_version']]
            allocations = [e for e in report['storage_events']
                           if e['action'] == 'allocate' and e['version'] == target]
            self.assertEqual(len(allocations), 1)
            self.assertLess(allocations[0]['cycle'], submit['issue'])
        installed = json.dumps(report['communication_installation'])
        self.assertNotIn('produce0', installed)
        self.assertNotIn('SharedTileReg', installed)
        self.assertNotIn('source_version', installed)
        self.assertTrue(report['noc']['drained'])

    def test_independent_core_compute_runs_between_acceptance_and_completion(self):
        from test_model import op, tile
        path = self.path / 'thread_0.jsonl'
        rows = [json.loads(line) for line in path.read_text().splitlines()]
        background = op('independent', deps=['comm:pair:0:submit'],
                        outputs=[tile('independent-result', 'independent', shape=(1, 64))])
        background.update(instance_op_sequence=2, order_dependencies=['consume0'])
        rows.append(background)
        path.write_text(''.join(json.dumps(row)+'\n' for row in rows))
        manifest_path = self.path / 'manifest.json'
        manifest = json.loads(manifest_path.read_text())
        manifest['instances'][0]['operation_count'] = 3
        manifest_path.write_text(json.dumps(manifest))
        report = self.run_model()
        timeline = {row['op_id']: row for row in report['timeline']}
        background = timeline['independent']
        self.assertGreater(background['issue'], timeline['comm:pair:0:submit']['complete'])
        self.assertLess(background['complete'], timeline['comm:pair:0:wait']['complete'])

    def test_core_source_lifetime_extends_past_acceptance(self):
        from frontend.capture import TraceAdapter
        from simulator import Simulator
        trace = TraceAdapter(self.path, self.config, self.plan)
        for entry in self.plan['phases'][0]['nodes']:
            trace.versions[tuple(entry['source_version'])].retained = False
        report = Simulator(trace, self.config).run()
        rows = {row['op_id']: row for row in report['timeline']}
        for entry in self.plan['phases'][0]['nodes']:
            submit = rows[f'comm:pair:{entry["core"]}:submit']
            free, = [event for event in report['storage_events']
                     if event['action'] == 'free' and event['version'] == entry['source_version']]
            self.assertGreater(free['cycle'], submit['source_read_complete'])
            self.assertGreater(free['cycle'], submit['complete'])


if __name__ == '__main__':
    unittest.main()
