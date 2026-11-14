"""Cycle driver shared by standalone and composed simulations."""
import json
from runtime.clock import Clock
from errors import ModelError


class Runner:
    def __init__(self, components, *, message_log=False, advance=None, tick_order=None):
        self.components = components
        self.advance = advance
        self.clock = Clock(components, message_log)
        self.tick_order = list(tick_order) if tick_order is not None else sorted(components)
        if len(self.tick_order) != len(components) or set(self.tick_order) != set(components):
            raise ValueError('tick_order must contain each component exactly once')
        self.cycle = 0

    def snapshot(self):
        return {'cycle': self.cycle, 'messages_in_flight': len(self.clock.pending),
                'components': {name: c.snapshot() for name, c in sorted(self.components.items())}}

    def run(self):
        while True:
            before = sum(c.progress for c in self.components.values())
            if self.advance is not None:
                for destination, message, time_ns in self.advance(self.cycle):
                    self.clock.external(destination, message, self.cycle, time_ns)
            # 固定周期协议：交付上一拍消息 → 每组件运行一次 → 收集本拍新消息。
            # collect 之前不会交付新消息，因此调整组件调用顺序不会产生同拍旁路。
            self.clock.deliver(self.cycle)
            for name in self.tick_order:
                self.components[name].tick(self.cycle)
            self.clock.collect(self.cycle)
            # 完成不只看计算结束：还必须等退休、消费通知及所有在途消息处理完。
            if not self.clock.pending and all(c.is_idle() for c in self.components.values()):
                return self.cycle
            progress = sum(c.progress for c in self.components.values()) > before
            future = any(c.next_event() is not None for c in self.components.values())
            # 无本地进展、无未来服务事件、无在途消息，才判为无法自行恢复的停滞。
            if not progress and not future and not self.clock.pending:
                raise ModelError('Deadlock at cycle ' + str(self.cycle) + ': ' +
                                 json.dumps(self.snapshot(), sort_keys=True))
            self.cycle += 1
