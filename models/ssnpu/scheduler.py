"""Dependency tracking, one-shot output reservations and fair dispatch only."""
from collections import Counter, defaultdict

from runtime.component import Component
from runtime.protocol import Admitted, Allocate, Allocated, Completed, Dispatch, Issued, core_name, unit_name


class Scheduler(Component):
    """waiting_admission → waiting_allocation → allocation_pending → dispatch_ready → issued。

    只管理依赖、输出预留和派发；执行单元收到命令后自行等待端口/计算资源。
    按线程程序顺序申请输出，预留完成后跳过数据未就绪指令，实现乱序发射。
    """
    def __init__(self, name, instructions, config):
        super().__init__(name)
        self.nodes = {i.id: i for i in instructions}
        self.pending = sorted(instructions, key=lambda i: (i.task, i.age, str(i.id)))
        self.status = {i.id: 'waiting_admission' for i in instructions}
        self.done = set()
        self.config, self.rotation = config, 0
        self.waits = defaultdict(Counter)
        self.blocked = {}

    def tick(self, cycle):
        # 1. 接收上一拍的反馈。Admitted/Completed 来自 ROB；Allocated 来自 TileReg。
        for _, message in self.inputs(cycle):
            if isinstance(message, Admitted):
                # 只有 ROB 准入通知能打开窗口；记录接收周期，与 ROB 的准入周期区分。
                assert self.status[message.instruction] == 'waiting_admission'
                self.status[message.instruction] = 'waiting_allocation'
                self.observe(message.instruction, admission_received=cycle, state='waiting_allocation')
            elif isinstance(message, Completed):
                self.done.add(message.instruction)
            elif isinstance(message, Allocated):
                # 分配成功仅表示输出容量已经保留，不表示指令已经执行或输出 ready。
                assert self.status[message.instruction] == 'allocation_pending'
                self.status[message.instruction] = 'dispatch_ready'
                self.observe(message.instruction, allocated=cycle, state='dispatch_ready')
            else:
                raise TypeError(message)
        # 2. 同线程按序发送一次性分配请求，不等数据或前一条 Allocated 往返。
        # 同步边仍限制跨阶段预留；TileReg 负责容量阻塞后的线程内排队。
        self.blocked = {}
        blocked_threads = {}
        for i in self.pending:
            state, thread = self.status[i.id], i.raw['_identity']
            if state not in ('waiting_admission', 'waiting_allocation'):
                continue
            if thread in blocked_threads:
                self.blocked[i.id] = f'waiting_prior_allocation:{blocked_threads[thread]}'
            elif state == 'waiting_admission' or not i.sync <= self.done:
                blocked_threads[thread] = i.id
            else:
                if i.alias:
                    # view 不占新容量；派发时仍检查真实数据及同步依赖。
                    self.status[i.id] = 'dispatch_ready'
                else:
                    self.status[i.id] = 'allocation_pending'
                    self.observe(i.id, allocation_request=cycle, state='allocation_pending')
                    # Scheduler → TileReg：指令 ID + 分配请求 ID；由 TileReg 查出全部输出。
                    self.send(core_name(i.core, 'tile'), Allocate(i.id, f'allocate:{i.id}'))
        counts, core_count = Counter(), 0
        n = self.config['parameters']['pe_count']['value']
        pe_width = self.config['parameters']['pe_issue_width']['value']
        core_width = self.config['parameters']['core_issue_width']['value']
        # 3. 每轮每 PE 最多派发一条，多轮填满本周期额度；每拍轮换起始 PE。
        # 完成预留且依赖就绪的命令参与仲裁；旧指令等数据时可被年轻指令绕过。
        # 无限命令队列允许提前派发；issue_width 限制派发数，计算吞吐由执行单元限制。
        progress = True
        while progress:
            progress = False
            for offset in range(n):
                pe = (self.rotation + offset) % n
                ready = next((i for i in self.pending if i.pe == pe and self.status[i.id] == 'dispatch_ready'
                              and (i.deps | i.sync) <= self.done
                              and (i.alias or (counts[pe] < pe_width and core_count < core_width))), None)
                if ready is None:
                    continue
                i = ready
                self.status[i.id] = 'issued'
                self.observe(i.id, issue=cycle, component=unit_name(i),
                             execution_component=unit_name(i), state='dispatched')
                # 同时发送两条消息：Issued 让 ROB 登记发射；Dispatch 让目标单元开始管理命令。
                # 两者都在下一拍交付；此处不会直接开始 Cube/Vector 的计算。
                self.send(core_name(i.core, 'rob'), Issued(i.id))
                self.send(unit_name(i), Dispatch(i.id))
                if not i.alias:
                    counts[pe] += 1
                    core_count += 1
                progress = True
        # 4. 已派发命令移交执行单元；只对仍未派发的指令记录本拍等待原因。
        self.pending = [i for i in self.pending if self.status[i.id] != 'issued']
        for i in self.pending:
            self.waits[str(i.id)]['|'.join(self.wait_reasons(i))] += 1
        self.rotation = (self.rotation + 1) % n

    def wait_reasons(self, i):
        state = self.status[i.id]
        if state == 'waiting_admission':
            return ['waiting_admission']
        reasons = []
        if not i.deps <= self.done:
            reasons.append('dependency')
        if not i.sync <= self.done:
            reasons.append('synchronization')
        if i.id in self.blocked:
            reasons.append(self.blocked[i.id])
        if state == 'allocation_pending':
            # 具体容量或前序阻塞由 TileReg 的 waits/snapshot 提供。
            reasons.append(state)
        elif state == 'dispatch_ready' and not reasons:
            reasons.append('issue_width')
        return sorted(reasons)

    def is_idle(self):
        return super().is_idle() and not self.pending

    def snapshot(self):
        return dict(super().snapshot(), waiting={str(i.id): dict(state=self.status[i.id],
                    reasons=self.wait_reasons(i),
                    dependencies=sorted(map(str, (i.deps | i.sync) - self.done))) for i in self.pending})
