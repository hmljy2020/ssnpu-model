"""Build core components without importing a network or workload frontend."""
from models.ssnpu.comm import CommPort
from models.ssnpu import Cube, Vector, TLSU, ROB, Scheduler, Sync, TileReg
from runtime.protocol import core_name, unit_name


def build_components(trace, config):
    nodes_all = trace.instructions
    components = {}
    cores = sorted({i.core for i in nodes_all.values()})
    subscribers = [core_name(c, unit) for c in cores for unit in ('scheduler', 'sync')]
    # 每个 ROB 向所有 Core 的 Scheduler/Sync 广播，跨 Core 的 task 依赖也能唤醒。
    for core in cores:
        nodes = {k: i for k, i in nodes_all.items() if i.core == core}
        real = [i for i in nodes.values() if not i.control]
        control = [i for i in nodes.values() if i.control]
        for unit, component in (
            ('scheduler', Scheduler(core_name(core, 'scheduler'), real, config)),
            ('rob', ROB(core_name(core, 'rob'), list(nodes.values()), subscribers,
                        config['parameters'].get('rob_depth', {}).get('value'))),
            ('tile', TileReg(core_name(core, 'tile'), nodes, trace.versions, config)),
            ('sync', Sync(core_name(core, 'sync'), control, config)),
            ('comm', CommPort(core_name(core, 'comm'),
                {i.id: i for i in real if unit_name(i) == core_name(core, 'comm')})),
            ('tlsu', TLSU(core_name(core, 'tlsu'),
                        {i.id: i for i in real if unit_name(i) == core_name(core, 'tlsu')}, config)),
        ):
            components[core_name(core, unit)] = component
        for pe in range(config['parameters']['pe_count']['value']):
            for unit, cls in (('cube', Cube), ('vector', Vector)):
                name = core_name(core, f'pe:{pe}/{unit}')
                components[name] = cls(name, {i.id: i for i in real if unit_name(i) == name}, config)
    return components
