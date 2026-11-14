"""Collect component observations and statistics without changing model state."""
from collections import Counter, defaultdict

from models.ssnpu import Cube, Vector, TLSU, ROB, Scheduler, TileReg
from models.ssnpu.comm import CommPort


def build_report(sim):
    # 仿真结束后只读汇总各组件 observe 打点；报告不参与时钟、消息或资源状态推进。
    nodes, cycle = sim.nodes, sim.cycle
    timeline = {k: dict(op_id=k, ir_op=i.raw['ir_op'], core=i.core, pe=i.pe,
                        queued=0, stages=[], accesses=[], transitions=[]) for k, i in nodes.items()}
    busy, byte_counts, usage, peak, stage_waits = (Counter() for _ in range(5))
    waits, observations, storage_events = defaultdict(Counter), [], []
    instruction_events = []
    milestones = {'admitted', 'admission_received', 'allocation_request', 'allocated', 'issue', 'received', 'operands_ready',
                  'compute_start', 'compute_end', 'writeback_start', 'writeback_end',
                  'complete', 'rob_complete', 'retire', 'accepted', 'source_read_complete'}
    # complete 是发送 Complete 的时刻，rob_complete 是 ROB 收到消息的时刻，retire 是按序退休。
    # 三者分别保留，便于区分执行延迟、消息传递和退休等待。
    for name, component in sorted(sim.components.items()):
        observations.extend((time, name, index, key, fields)
                            for index, (time, key, fields) in enumerate(component.observations))
        if hasattr(component, 'service'):
            busy.update(component.service.busy)
            byte_counts.update(component.service.bytes)
        if isinstance(component, TileReg):
            usage.update(component.usage)
            peak.update(component.peak)
            storage_events.extend(component.storage_events)
        if isinstance(component, (Scheduler, TileReg)):
            for key, counts in component.waits.items():
                waits[key].update(counts)
        if isinstance(component, (Cube, Vector, TLSU, CommPort)):
            stage_waits.update(component.waits)
    for time, name, _, key, fields in sorted(observations, key=lambda r: r[:3]):
        row = timeline[key]
        for field, value in fields.items():
            if field in ('stage', 'access'):
                row['stages' if field == 'stage' else 'accesses'].append(value)
            else:
                row[field] = value
            if field in milestones:
                i = nodes[key]
                instruction_events.append(dict(cycle=time, event=field, op_id=key, ir_op=i.raw['ir_op'],
                                               core=i.core, pe=i.pe, task=i.task, block=i.block,
                                               thread=i.raw.get('thread_id'), control=i.control, component=name))
        if 'state' in fields:
            row['transitions'].append(dict(cycle=time, component=name, state=fields['state']))
    core_end, pe_end, pe_tile_end = {}, {}, {}
    for key, row in timeline.items():
        row['stages'].sort(key=lambda s: (s['start'], s['resource']))
        core = str(row['core'])
        core_end[core] = max(core_end.get(core, 0), row['complete'])
        if not nodes[key].control:
            pe = f'{core}:{row["pe"]}'
            pe_tile_end[pe] = max(pe_tile_end.get(pe, 0), row['complete'])
            pe_end[pe] = max(pe_end.get(pe, 0), row['complete'])
        elif nodes[key].raw['ir_op'] == 'barrier':
            for c, p in sim.trace.block_members[nodes[key].task, nodes[key].block]:
                pe = f'{c}:{p}'
                pe_end[pe] = max(pe_end.get(pe, 0), row['complete'])
    for event in instruction_events:
        event['execution_component'] = timeline[event['op_id']]['execution_component']
    report = {
        'total_cycles': cycle,
        'last_instruction_complete_cycle': max((r['complete'] for k, r in timeline.items() if not nodes[k].control), default=0),
        'last_node_complete_cycle': max((r['complete'] for r in timeline.values()), default=0),
        'instruction_count': sum(not i.control for i in nodes.values()),
        'core_completion': core_end, 'pe_completion': pe_end, 'pe_tile_completion': pe_tile_end,
        'source_sha256': sim.trace.source_hashes,
        'timeline': list(timeline.values()),
        'instruction_events': instruction_events,
        'resources': {k: dict(service_cycles=v, utilization=v / cycle if cycle else 0, bytes=byte_counts[k])
                      for k, v in sorted(busy.items())},
        'storage_peak_bytes': dict(peak), 'storage_live_bytes': dict(usage),
        'storage_events': sorted(storage_events, key=lambda e: e['cycle']),
        'wait_reason_sets': {k: dict(v) for k, v in waits.items()},
        'internal_stage_resource_wait_cycles': dict(stage_waits),
        'rob_windows': {name: component.window_stats() for name, component in sorted(sim.components.items())
                        if isinstance(component, ROB)},
        'components': sim.snapshot()['components'],
        'config': sim.config,
        'assumptions': [
            'Synthetic rates; hardware cycle accuracy has not been calibrated.',
            'Per-thread rob_depth limits admitted but not retired instructions; null is unlimited.',
            'Views occupy ROB slots; synthetic control nodes bypass admission via Sync.',
            'ROB full_cycles counts post-refill cycles with a full window and pending admission.',
            'order_dependencies establish age, not completion.',
            ('Core messages arrive next core cycle; network events use exact time, '
             'with CDC and ceiling to the receiving core edge.') if sim.noc is not None else
            'Components tick once per cycle; every message is delivered next cycle.',
            'Ordered operand handshakes; ready commands arbitrate independently by age.',
            'Private TileReg service included in compute cost; message round trips remain.',
            'Shared reads charged separately per consumer; no broadcast combining.',
            'Outputs atomically reserved in thread order before data readiness; synchronization gates reservations.',
            'Reserved instructions dispatch out of order after data and synchronization dependencies complete.',
            'Local dynamic versions occupy independent capacity; only Shared fixed slots await prior readers.',
            'Local release waits for producer and all consumers to complete; this is a coarse approximation, '
            'not the full SSM retirement and early-release model.',
            'Views use messages without tile quota or new storage; reuse-only shared versions remain live-out.',
            'Completion broadcasts do not wait for in-order thread retirement.',
            'Total cycles include retirement, consumption and message drain.',
            'Memory paths use configured costs, without cache residency simulation.',
            'Wait reason sets count instruction-cycles; overlapping categories are not additive.'],
    }
    if sim.clock.log_messages:
        report['messages'] = sim.clock.messages
    if 'wse' in sim.config:
        report['total_time_ns'] = cycle * 1e9 / sim.config['wse']['core_clock_hz']['value']
    if sim.hbm is not None:
        report['hbm_events'] = sim.hbm.events
        report['assumptions'].append(
            'HBM reads and writes share chip bandwidth equally across transferring cores, capped per core. '
            'Each core queues its own transfers; startup can overlap across cores and uses no chip bandwidth. '
            'Shares change on core clock edges; SRAM endpoint service is charged separately.')
    if sim.noc is not None:
        report['noc'] = sim.noc.report()
        report['communication_plan'] = sim.trace.communication_plan
        report['communication_installation'] = sim.trace.comm_installation
        elapsed = cycle * sim.noc.core_ns
        report['total_time_ns'] = float(elapsed)
        report['total_time_ns_exact'] = str(elapsed)
    return report
