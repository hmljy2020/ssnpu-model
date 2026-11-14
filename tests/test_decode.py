"""Exercise PTO replay and HBM timing independently of a kernel generator."""
import copy
import io
import json
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from tempfile import TemporaryDirectory

from analysis.pipeline import service_intervals
from api import ModelError, simulate
from config import default_config, validate_config, wse_config
from examples.decode.run import main
from models.memory.hbm import HBM
from runtime.protocol import HBMRequest
from test_model import ROOT, fixture, op, simulate as simulate_fixture


class DecodeReplayTests(unittest.TestCase):
    def setUp(self):
        workspace = ROOT / 'workspace'
        workspace.mkdir(exist_ok=True)
        tmp = TemporaryDirectory(dir=workspace)
        self.addCleanup(tmp.cleanup)
        self.path = Path(tmp.name)

    def test_replay_archived_pypto_fa_without_generating_a_capture(self):
        capture = ROOT / 'examples/fa/capture'
        config = default_config()
        config['wse'] = wse_config()['wse']
        config_path = self.path / 'config.json'
        config_path.write_text(json.dumps(config))
        output = self.path / 'replay'
        with redirect_stdout(io.StringIO()):
            main([str(capture), '--config', str(config_path), '--no-plots', '--output-dir', str(output)])
        report = json.loads((output / 'report.json').read_text())
        self.assertEqual(report['instruction_count'], 320)
        direct = simulate(capture)
        self.assertEqual(report['total_cycles'], direct['total_cycles'])
        self.assertEqual(report['source_sha256'], direct['source_sha256'])
        self.assertTrue((output / 'pipeline.pftrace').stat().st_size)
        self.assertFalse((output / 'capture').exists())

    def test_public_replay_accepts_synthetic_records(self):
        path = fixture(self.path / 'synthetic', [op(0)])
        self.assertEqual(simulate(path)['instruction_count'], 1)

    def test_hbm_arbitration_and_bandwidth(self):
        # Two cores each copy 128 FP16 elements from HBM to local SRAM.
        # The only variable between simulations is the configured chip bandwidth.
        rows = []
        for core in range(2):
            source = dict(object_id=f'src{core}', kind='tensor', shape=[1, 128],
                          dtype='float16', memory_space='HBM')
            target = dict(source, object_id=f'dst{core}', memory_space='SRAM')
            row = op(core, pe=core, inputs=[source], outputs=[target], name='memory.copy')
            row['memory_accesses'] = [dict(object_id=obj['object_id'], shape=obj['shape'])
                                      for obj in (source, target)]
            rows.append(row)
        capture = fixture(self.path / 'hbm', rows)
        manifest_path = capture / 'manifest.json'
        manifest = json.loads(manifest_path.read_text())
        for instance in manifest['instances']:
            instance['mapping'] = dict(core_id=instance['thread_id'], pe_id=0)
        manifest_path.write_text(json.dumps(manifest))
        config = wse_config()
        fast = simulate_fixture(capture, config)
        slow_config = copy.deepcopy(config)
        slow_config['hbm'] = dict(bandwidth_bytes_per_second=256e9, startup_ns=0)
        slow = simulate_fixture(capture, slow_config)
        for report, cycles in ((fast, 2), (slow, 3)):
            first, second = report['hbm_events']
            self.assertEqual(first['start_cycle'], second['start_cycle'])
            self.assertEqual(first['end_cycle'], second['end_cycle'])
            self.assertEqual(first['end_cycle'] - first['start_cycle'], cycles)
            self.assertEqual(report['resources']['chip/hbm']['service_cycles'], cycles)
            self.assertEqual(report['resources']['chip/hbm']['bytes'], 512)
            hbm_lanes = {e['lane'] for e in service_intervals(report) if e['kind'] == 'HBM'}
            self.assertEqual(hbm_lanes, {'chip/hbm/core:0', 'chip/hbm/core:1'})
            for row in report['timeline']:
                if row['ir_op'] == 'memory.copy':
                    hbm, sram = row['stages']
                    self.assertLess(hbm['service_end'], sram['start'])
        self.assertGreater(slow['total_cycles'], fast['total_cycles'])


class HBMBandwidthTests(unittest.TestCase):
    def run_hbm(self, config, arrivals):
        hbm = HBM(validate_config(config))
        for cycle in range(100):
            for request in arrivals.get(cycle, ()):
                hbm.receive('commands', request)
            hbm.tick(cycle)
            hbm.drain_outputs()
            if cycle >= max(arrivals) and hbm.is_idle():
                return hbm
        self.fail('HBM failed to drain')

    def test_all_40_cores_transfer_concurrently_under_both_limits(self):
        arrivals = {0: [HBMRequest(core, core, 3200, 'read', True) for core in range(40)]}
        # At 1.4 GHz the per-core cap binds. At 2.5 GHz the same 10 TB/s caps each core at 100 B/cycle.
        for frequency, cycles in ((1_400_000_000, 25), (2_500_000_000, 32)):
            with self.subTest(frequency=frequency):
                config = wse_config()
                config['wse']['core_clock_hz']['value'] = frequency
                hbm = self.run_hbm(config, arrivals)
                self.assertEqual({e['core'] for e in hbm.events}, set(range(40)))
                self.assertEqual({(e['start_cycle'], e['end_cycle']) for e in hbm.events}, {(0, cycles)})
                self.assertEqual(hbm.service.bytes['chip/hbm'], 128000)
                self.assertEqual(hbm.service.busy['chip/hbm'], cycles)

    def test_share_changes_on_arrival_and_completion_and_counts_each_core_once(self):
        # At 1 GHz this yields 128 B/cycle with one active core and 96 B/cycle with two.
        arrivals = {
            0: [HBMRequest('long', 0, 640, 'read', True)],
            1: [HBMRequest('queued', 0, 128, 'read', True),
                HBMRequest('short', 1, 192, 'write', False)],
        }
        for startup in (0, 2):
            with self.subTest(startup_ns=startup):
                config = wse_config()
                config['wse']['core_clock_hz']['value'] = 1_000_000_000
                config['hbm'] = dict(bandwidth_bytes_per_second=192e9, startup_ns=startup)
                hbm = self.run_hbm(config, arrivals)
                spans = {e['op_id']: (e['start_cycle'], e['end_cycle']) for e in hbm.events}
                self.assertEqual(spans, {
                    'long': (startup, 6 + startup),
                    'short': (1 + startup, 3 + startup),
                    'queued': (6 + 2 * startup, 7 + 2 * startup),
                })
                self.assertEqual(hbm.service.busy['chip/hbm'], 7)
