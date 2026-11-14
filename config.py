"""Synthetic model parameters and configuration validation."""
from __future__ import annotations

import math

from errors import ModelError


PARAMETERS = {
    'pe_count': (4, 'PE', 'Core', 'Number of independent compute engines'),
    'pe_issue_width': (2, 'tile/cycle', 'PE', 'Maximum tile issues per cycle'),
    'core_issue_width': (4, 'tile/cycle', 'Core', 'Maximum tile issues per cycle'),
    'rob_depth': (None, 'instruction', 'thread', 'Maximum admitted but not retired instructions'),
    'local_capacity': (1048576, 'byte', 'PE', 'LocalTileReg capacity'),
    'shared_capacity': (1048576, 'byte', 'Core', 'SharedTileReg capacity'),
    'barrier_latency': (2, 'cycle', 'block', 'Full phase synchronization delay'),
}


ENGINES = {
    'cube': (4096, 4, 'MAC/cycle', 'PE'),
    'vector': (256, 2, 'element/cycle', 'PE'),
    'exp': (64, 4, 'element/cycle', 'PE'),
    'cast': (256, 2, 'element/cycle', 'PE'),
    'reduce': (128, 3, 'reduction/cycle', 'PE'),
    'memory': (256, 8, 'byte/cycle', 'Core'),
    'shared_read': (256, 2, 'byte/cycle', 'Core'),
    'shared_write': (256, 2, 'byte/cycle', 'Core'),
}

HBM_DEFAULTS = dict(bandwidth_bytes_per_second=10_000_000_000_000,
                    per_core_bytes_per_cycle=128, startup_ns=0)


def default_config():
    return {
        'name': 'logic-validation-only',
        'message_log': False,
        'parameters': {k: {'value': v, 'unit': u, 'scope': s, 'meaning': m,
                           'source': 'synthetic logical validation; uncalibrated'}
                       for k, (v, u, s, m) in PARAMETERS.items()},
        'engines': {k: {'throughput': t, 'latency': l, 'unit': u, 'scope': s,
                        'meaning': 'Service rate and result latency after service',
                        'source': 'synthetic logical validation; uncalibrated',
                        'dtype_overrides': {}} for k, (t, l, u, s) in ENGINES.items()},
        'paths': {'DDR': 'memory', 'L1': 'memory', 'L2': 'memory'},
    }


def validate_config(c):
    try:
        if type(c.get('message_log', False)) is not bool:
            raise ModelError('message_log must be a boolean')
        # 旧配置缺省为无限容量；显式非法值仍走下面的严格校验。
        if 'rob_depth' not in c['parameters']:
            c['parameters']['rob_depth'] = default_config()['parameters']['rob_depth']
        for k in PARAMETERS:
            v = c['parameters'][k]['value']
            if k == 'rob_depth' and v is None:
                continue
            if type(v) is not int or v < (0 if k == 'barrier_latency' else 1):
                raise ModelError(f'Invalid parameter {k}: {v}')
        for k, (_, _, _, scope) in ENGINES.items():
            e = c['engines'][k]
            allowed_scopes = {scope, 'Core'} if 'wse' in c and k in ('cube', 'vector', 'exp', 'cast', 'reduce') else {scope}
            if e['scope'] not in allowed_scopes:
                raise ModelError(f'Invalid scope for {k}')
            for v in [e] + list(e.get('dtype_overrides', {}).values()):
                rate, latency = v.get('throughput', e['throughput']), v.get('latency', e['latency'])
                if not isinstance(rate, (int, float)) or not math.isfinite(rate) or rate <= 0:
                    raise ModelError(f'Invalid throughput for {k}')
                if type(latency) is not int or latency < 0:
                    raise ModelError(f'Invalid latency for {k}')
        for path, engine in c['paths'].items():
            if isinstance(engine, dict):
                if engine.get('engine') != 'memory' or not isinstance(engine.get('throughput', 1), (int, float)) or not math.isfinite(engine.get('throughput', 1)) or engine.get('throughput', 1) <= 0 or type(engine.get('latency', 0)) is not int or engine.get('latency', 0) < 0:
                    raise ModelError(f'Invalid memory path {path}: {engine}')
            elif engine != 'memory':
                raise ModelError(f'Unsupported path engine {path}: {engine}')
    except (KeyError, TypeError) as exc:
        raise ModelError(f'Incomplete configuration: {exc}') from exc
    if "wse" in c:
        validate_wse(c)
    if 'hbm' in c:
        if 'wse' not in c:
            raise ModelError('HBM requires a WSE clock configuration')
        if not isinstance(c['hbm'], dict):
            raise ModelError('Expected an HBM configuration object')
        hbm = c['hbm'] = HBM_DEFAULTS | c['hbm']
        for key, minimum in (('bandwidth_bytes_per_second', 1), ('per_core_bytes_per_cycle', 1),
                             ('startup_ns', 0)):
            value = hbm[key]
            if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value) or value < minimum:
                raise ModelError(f'Invalid HBM parameter {key}')
    return c


# Values retain their provenance; none of the pending values are hardware calibration.
WSE_PARAMETERS = {
    'core_clock_hz': (1400000000, 'Hz', 'documented'),
    'noc_clock_hz': (2000000000, 'Hz', 'documented'),
    'link_bytes_per_cycle': (64, 'B/NoC cycle/direction', 'documented'),
    'header_bytes': (12, 'B/flit', 'pending'),
    'hop_cycles': (1, 'NoC cycle', 'pending'),
    'tx_cdc_cycles': (2, 'NoC cycle', 'pending'),
    'rx_cdc_cycles': (2, 'NoC cycle', 'pending'),
    'alignment_cycles': (16, 'NoC cycle', 'pending'),
    'arm_cycles': (8, 'NoC cycle', 'pending'),
    'guard_cycles': (1, 'NoC cycle', 'pending'),
    'slot_cycles': (1024, 'NoC cycle', 'pending'),
    'slots_per_period': (8, 'slots', 'pending'),
    'epoch_bits': (8, 'bit', 'pending'),
    'sram_capacity_bytes': (384 * 1024 * 1024, 'B/core', 'documented'),
    'sram_bandwidth': (1000000000000, 'B/s/core, shared read/write', 'configured'),
    'sram_read_startup_ns': (100, 'ns', 'configured'),
    'sram_write_startup_ns': (100, 'ns', 'configured'),
}


def wse_config():
    c = default_config()
    c['name'] = 'wse40-shared-tile-pending'
    c['hbm'] = HBM_DEFAULTS.copy()
    c['wse'] = {k: dict(value=v, unit=u, status=s,
                         source=('WSE whitepaper v0.2' if s == 'documented' else
                                 'user-confirmed first-version model; 2026-11-13'))
                for k, (v, u, s) in WSE_PARAMETERS.items()}
    # WSE targets are whole-core budgets, statically partitioned across PEs.
    # Keep default_config's synthetic per-PE profile unchanged.
    for name in ('cube', 'vector', 'exp', 'cast', 'reduce'):
        c['engines'][name]['scope'] = 'Core'
        c['engines'][name]['meaning'] = 'Whole-core budget, equally partitioned across configured PEs'
    c['engines']['cube']['source'] = 'WSE whitepaper v0.2: FP16 4096 MAC/cycle/core target'
    for name in ('vector', 'cast'):
        c['engines'][name]['unit'] = 'byte/cycle'
        c['engines'][name]['source'] = 'WSE whitepaper v0.2: 256 B/cycle/core target; equal PE partition assumed'
    # exp/reduce retain synthetic operation rates, now aggregate and additionally
    # capped by the whole-core vector byte budget in Vector.start.
    for k in ('local_capacity', 'shared_capacity'):
        c['parameters'][k]['status'] = 'pending'
    for k in ('shared_read', 'shared_write'):
        c['engines'][k]['status'] = 'pending'
    # A capture must explicitly identify local memory; no DDR/HBM reinterpretation.
    c['paths'] = {'SRAM': 'memory'}
    return c


def validate_wse(c):
    p = c['wse']
    if set(p) != set(WSE_PARAMETERS):
        raise ModelError('WSE parameter keys differ from the supported profile')
    zero_ok = {'tx_cdc_cycles', 'rx_cdc_cycles', 'alignment_cycles', 'arm_cycles',
               'guard_cycles', 'sram_read_startup_ns', 'sram_write_startup_ns'}
    for k, entry in p.items():
        value = entry['value']
        if type(value) is not int or value < (0 if k in zero_ok else 1):
            raise ModelError(f'Invalid WSE parameter {k}: {value}')
        if entry.get('status') not in {'documented', 'pending', 'configured'} or not entry.get('source'):
            raise ModelError(f'Missing WSE provenance: {k}')
    if p['header_bytes']['value'] >= p['link_bytes_per_cycle']['value']:
        raise ModelError('No flit payload capacity')
    if c['engines']['shared_write'].get('dtype_overrides'):
        raise ModelError('WSE reservations require one dtype-independent shared write service')

def platform_spec(config):
    """Only network clocks/rates and declared endpoint write service cross over."""
    return dict(network={k: v['value'] for k, v in config['wse'].items() if not k.startswith('sram_')},
                write_service={k: config['engines']['shared_write'][k] for k in ('throughput', 'latency')},
                endpoint_command_cycles=1, reservation_roundtrip_cycles=4)
