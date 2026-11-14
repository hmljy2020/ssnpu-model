"""Replay a PTO decode capture and export simulator timelines."""
import argparse
import json
from pathlib import Path
import sys

from api import simulate
from analysis.pipeline import export_pipeline, export_resource_trace
from analysis.perfetto import to_pftrace, to_perfetto
from errors import ModelError
from frontend.capture import TraceAdapter
from config import platform_spec


def export_communication(trace, output):
    directory = output / 'communication'
    directory.mkdir(exist_ok=True)
    requests = [i.raw['communication']['request'] for i in trace.instructions.values()
                if i.raw['ir_op'] == 'comm.submit']
    for name, value in (('requests.json', requests), ('installation.json', trace.comm_installation),
                        ('platform.json', platform_spec(trace.config))):
        (directory/name).write_text(json.dumps(value, indent=2, allow_nan=False)+'\n')


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('capture_dir', type=Path, help='PyPTO workload capture directory')
    parser.add_argument('--config', type=Path, required=True, help='WSE hardware configuration JSON')
    parser.add_argument('--communication-plan', type=Path, help='Explicit communication bindings for the capture')
    parser.add_argument('--single-core-capture', type=Path, help='Optional separate single-core PyPTO capture')
    parser.add_argument('--output-dir', type=Path, default=Path('workspace/decode'))
    parser.add_argument('--no-plots', action='store_true')
    args = parser.parse_args(argv)
    try:
        config = json.loads(args.config.read_text())
        if not isinstance(config, dict) or 'wse' not in config:
            raise ModelError('Decode timeline export requires a WSE configuration')
        report = simulate(args.capture_dir, config, communication_plan=args.communication_plan)
        single_report = None
        if args.single_core_capture is not None:
            single_report = simulate(args.single_core_capture, config)
            if len(single_report['core_completion']) != 1:
                raise ModelError('--single-core-capture must map to exactly one core')
    except (ModelError, OSError, json.JSONDecodeError) as exc:
        parser.exit(2, f'{exc}\n')

    output = args.output_dir
    output.mkdir(parents=True, exist_ok=True)
    if 'communication_installation' in report:
        trace = TraceAdapter(args.capture_dir, report['config'], args.communication_plan)
        export_communication(trace, output)
    cycle_ns = 1e9/config['wse']['core_clock_hz']['value']
    (output/'pipeline.pftrace').write_bytes(to_pftrace(report['instruction_events'], cycle_ns=cycle_ns))
    (output/'pipeline.trace.json').write_text(json.dumps(to_perfetto(report['instruction_events'],cycle_ns=cycle_ns))+'\n')
    (output/'instruction_events.jsonl').write_text(''.join(json.dumps(e)+'\n' for e in report['instruction_events']))
    (output/'hbm_events.jsonl').write_text(''.join(json.dumps(e)+'\n' for e in report.get('hbm_events', [])))
    (output/'noc_events.jsonl').write_text(''.join(json.dumps(e)+'\n' for e in report.get('noc',{}).get('events',[])))
    export_resource_trace(report, output)
    summary = dict(capture=str(args.capture_dir), source_sha256=report['source_sha256'],
                   total_cycles=report['total_cycles'], total_time_us=report['total_time_ns']/1000,
                   instruction_count=report['instruction_count'],
                   hbm_bytes=report['resources'].get('chip/hbm', {}).get('bytes', 0),
                   software=dict(python=sys.version))
    if single_report is not None:
        summary['single_core_time_us'] = single_report['total_time_ns']/1000
        (output/'single_core_report.json').write_text(json.dumps(single_report, indent=2, allow_nan=False)+'\n')
    for name, value in (('report.json', report), ('summary.json', summary), ('config.json', report['config'])):
        (output/name).write_text(json.dumps(value,indent=2,allow_nan=False)+'\n')
    if not args.no_plots:
        export_pipeline(report,output)
    print(json.dumps({key: summary[key] for key in ('total_cycles', 'total_time_us', 'hbm_bytes')}, indent=2))


if __name__ == '__main__':
    main()
