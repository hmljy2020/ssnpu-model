"""Compose independent core and network components with a common runner."""
from config import platform_spec
from errors import ModelError
from models.ssnpu.system import build_components
from runtime.runner import Runner


class Simulator(Runner):
    def __init__(self, trace, config, tick_order=None, *, core_only=False, comm_backend=None):
        self.trace, self.config = trace, config
        self.nodes = trace.instructions
        components = build_components(trace, config)
        self.noc = None
        self.hbm = None
        if core_only:
            if 'hbm' in config:
                raise ModelError('simulate_core requires a core configuration without HBM')
            if any(i.raw['ir_op'].startswith('comm.') for i in self.nodes.values()) and comm_backend is None:
                raise ModelError('Core communication instructions require comm_backend')
        elif 'hbm' in config:
            from models.memory.hbm import HBM
            self.hbm = HBM(config)
            components['hbm'] = self.hbm
        if comm_backend is not None:
            components['communit'] = comm_backend
        elif not core_only and trace.calendar_phases:
            from models.noc.unit import CommUnit
            self.noc = CommUnit(trace.comm_installation, platform_spec(config))
            components['communit'] = self.noc
        backend = components.get('communit')
        super().__init__(components, message_log=config.get('message_log', False),
                         advance=getattr(backend, 'advance', None), tick_order=tick_order)

    def run(self):
        super().run()
        from analysis.report import build_report
        return build_report(self)
