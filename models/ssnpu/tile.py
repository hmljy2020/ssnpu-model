"""Sole owner of storage, readiness, consumers and shared access ports."""
import math
from collections import Counter, defaultdict

from runtime.component import Component
from runtime.protocol import (Access, AccessDone, Allocate, Allocated, Complete, Consumed,
                       Dispatch, core_name, unit_name)
from runtime.service import Service
from errors import ModelError
from runtime.protocol import (ReceivePrepare, PortFence, WriteReservations, RouterWrite,
                       PublishReceive, TransferReply)


class TileReg(Component):
    """Core 内 local/shared 存储的唯一状态管理者。

    active 表示占用容量，ready 表示版本可读，done 表示生产/消费指令已完成。
    allocations 等容量，accesses 等 ready/端口，running 等共享访问服务返回。
    """
    def __init__(self, name, instructions, versions, config):
        super().__init__(name)
        self.nodes, self.versions, self.config = instructions, versions, config
        self.active = set()
        self.usage, self.peak = Counter(), Counter()
        self.storage_events = []
        self.service = Service()
        self.ready, self.done = set(), set()
        self.allocations, self.accesses, self.running = [], [], []
        self.blocked = {}
        self.receives, self.reservations = {}, {}
        self.port_fences, self.router_running = set(), []
        self.waits = defaultdict(Counter)

    def finish_instruction(self, key):
        # 消费完成按整条指令计账，保守地保留输入直到输出写回结束。
        # consumers 是 trace 中该版本的全部消费者，包含还没派发的命令。
        i = self.nodes[key]
        self.done.add(key)
        for vk in sorted(i.inputs | i.outputs, key=str):
            v = self.versions[vk]
            if not v.retained and v.producer in self.done and v.consumers <= self.done:
                # retained 表示需要继续保留的共享版本；其余版本在生产及全部消费结束后回收。
                self.release(vk)
                self.ready.discard(vk)

    def release(self, key):
        if key not in self.active:
            return
        self.active.remove(key)
        v = self.versions[key]
        self.usage[v.pool] -= v.size
        self.storage_events.append(dict(cycle=self.cycle, action='free', version=list(key), pool=v.pool, bytes=v.size))

    def allocate(self, instruction):
        if instruction.raw['ir_op'] == 'comm.submit':
            instruction = self.nodes[instruction.raw['communication']['reserve_for']]
        # Submit reserves the wait producer's output; wait must not allocate it twice.
        if instruction.raw['ir_op'] == 'comm.wait' and instruction.outputs <= self.active:
            return None
        # 所有动态版本独立占容量；全部输出检查通过后才原子预留。
        delta = Counter()
        for key in sorted(instruction.outputs, key=str):
            v = self.versions[key]
            delta[v.pool] += v.size
        for pool, amount in delta.items():
            limit = self.config['parameters']['shared_capacity' if pool.endswith('/shared') else 'local_capacity']['value']
            if self.usage[pool] + amount > limit:
                return f'capacity:{pool}'
        for key in sorted(instruction.outputs, key=str):
            v = self.versions[key]
            self.active.add(key)
            self.usage[v.pool] += v.size
            self.peak[v.pool] = max(self.peak[v.pool], self.usage[v.pool])
            self.storage_events.append(dict(cycle=self.cycle, action='allocate', version=list(key), pool=v.pool, bytes=v.size))
        return None

    def start_access(self, request):
        # shared 读写各用独立 Core 端口，按字节/周期计费；不同消费者分别占用读带宽。
        engine = 'shared_write' if request.write else 'shared_read'
        e = self.config['engines'][engine]
        cost = e | e.get('dtype_overrides', {}).get(request.dtype, {})
        i = self.nodes[request.instruction]
        duration = max(1, math.ceil(request.work / cost['throughput']))
        if request.write and (self.port_fences or any(
                self.cycle < start + service and start < self.cycle + duration
                for start, service, _ in self.reservations.values())):
            return None
        return self.service.start(core_name(i.core, engine), engine, request.work,
                                  max(1, math.ceil(request.work / cost['throughput'])), cost['latency'], self.cycle)

    def respond(self, request):
        if request.write:
            # 写完成才发布版本 ready；Allocated 仅预留容量，不会到这里。
            self.ready.add(request.version)
        # TileReg → 原执行单元：仅返回指令/请求 ID，通知对应读或写已完成，不携带数值。
        self.send(unit_name(self.nodes[request.instruction]),
                  AccessDone(request.instruction, request.request), 'access')

    def transfer_message(self, message):
        if not isinstance(message, (ReceivePrepare, PortFence, WriteReservations, RouterWrite, PublishReceive)):
            return False
        key = message.instruction
        i = self.nodes[key]
        version, = i.outputs
        if version not in self.active:
            raise ModelError(f'Calendar accesses an inactive generation: {key}')
        if isinstance(message, ReceivePrepare):
            if message.version != version or key in self.receives:
                raise ModelError('Invalid or duplicate receive registration')
            expected = {source: (offset, size) for source, offset, size in message.expected}
            cursor = 0
            for offset, size in sorted(expected.values()):
                if offset != cursor or size <= 0:
                    raise ModelError('Overlapping or incomplete receive authorization')
                cursor += size
            if cursor != self.versions[version].size or len(expected) != len(message.expected):
                raise ModelError('Invalid receive coverage')
            self.receives[key] = dict(version=version, expected=expected, pending=set(), committed=set(), published=False)
            self.send(core_name(i.core, 'comm'), TransferReply(key, 'posted'))
        elif isinstance(message, PortFence):
            if key not in self.receives:
                raise ModelError('Port fence before receive registration')
            self.port_fences.add(key)
            available = max(self.cycle, self.service.available[core_name(i.core, 'shared_write')])
            self.send(core_name(i.core, 'comm'), TransferReply(key, 'port_ready', available))
        elif isinstance(message, WriteReservations):
            if key not in self.port_fences:
                raise ModelError('Write reservation without admission fence')
            expected = self.receives[key]['expected']
            if {entry[0] for entry in message.entries} != set(expected):
                raise ModelError('Reservation coverage differs from receive coverage')
            for source, start, service, latency in message.entries:
                if (key, source) in self.reservations or start < self.cycle:
                    raise ModelError('Duplicate or late write reservation')
                for old_start, old_service, _ in self.reservations.values():
                    if start < old_start + old_service and old_start < start + service:
                        raise ModelError('Shared write reservations overlap')
                self.reservations[key, source] = start, service, latency
            self.port_fences.remove(key)
            self.send(core_name(i.core, 'comm'), TransferReply(key, 'reserved'))
        elif isinstance(message, RouterWrite):
            ctx = self.receives[key]
            if message.version != version or ctx['version'] != version:
                raise ModelError('Stale router write generation')
            if ctx['expected'].get(message.source) != (message.offset, message.work):
                raise ModelError('Router write is outside its authorized segment')
            if message.source in ctx['pending'] | ctx['committed']:
                return True  # Identical duplicates, including pending writes, never count twice.
            start, service, latency = self.reservations.pop((key, message.source))
            if self.cycle != start:
                raise ModelError('Router missed its reserved write window')
            event = self.service.start(core_name(i.core, 'shared_write'), 'shared_write',
                                       message.work, service, latency, self.cycle)
            if event is None:
                raise ModelError('Reserved Shared write port is busy')
            ctx['pending'].add(message.source)
            self.observe(key, stage=event)
            self.router_running.append((message, event['complete']))
        elif isinstance(message, PublishReceive):
            ctx = self.receives[key]
            if ctx['published'] or ctx['pending'] or ctx['committed'] != set(ctx['expected']):
                raise ModelError('Premature or duplicate receive publication')
            self.ready.add(version)
            ctx['published'] = True
            self.send(core_name(i.core, 'comm'), TransferReply(key, 'published'))
        return True

    def tick(self, cycle):
        # 1. 收取请求和消费通知。Allocate/Access 入等待队列，在本组件内自主重试。
        for _, message in self.inputs(cycle):
            try:
                if self.transfer_message(message):
                    continue
            except ModelError as exc:
                self.send(core_name(self.nodes[message.instruction].core, 'comm'),
                          TransferReply(message.instruction, 'fault', str(exc)))
                continue
            if isinstance(message, Allocate):
                self.allocations.append(message)
            elif isinstance(message, Access):
                i = self.nodes[message.instruction]
                assert message.version in (i.outputs if message.write else i.inputs), message
                self.accesses.append(message)
            elif isinstance(message, Consumed):
                self.finish_instruction(message.instruction)
            elif isinstance(message, Dispatch):
                i = self.nodes[message.instruction]
                assert i.alias
                self.observe(i.id, complete=cycle, state='complete')
                # view 由 TileReg 直接完成：别名已在 trace 适配时绑定，无需计算或新分配。
                # TileReg → ROB 发送 Complete，同时在本地登记该 view 已完成。
                self.send(core_name(i.core, 'rob'), Complete(i.id))
                self.finish_instruction(i.id)
            else:
                raise TypeError(message)
        for message, end in list(self.router_running):
            if end <= cycle:
                ctx = self.receives[message.instruction]
                ctx['pending'].remove(message.source)
                ctx['committed'].add(message.source)
                self.send(core_name(self.nodes[message.instruction].core, 'comm'), TransferReply(message.instruction, 'written', (message.source, end)))
                self.router_running.remove((message, end))
        # 2. 返回到期的共享访问；respond 发送 AccessDone，执行单元下一拍才能消费。
        for request, end in list(self.running):
            if end <= cycle:
                self.respond(request)
                self.running.remove((request, end))
        # 3. 同线程按序分配，受阻后禁止年轻申请抢占；其他线程仍可推进。
        # 同拍可连续满足多个请求，不增加逐条消息往返的吞吐限制。
        self.blocked = {}
        blocked_threads = {}
        for request in sorted(self.allocations, key=lambda r: (self.nodes[r.instruction].task,
                                                               self.nodes[r.instruction].age,
                                                               str(r.instruction))):
            i = self.nodes[request.instruction]
            thread = i.raw['_identity']
            reason = (f'waiting_prior_allocation:{blocked_threads[thread]}' if thread in blocked_threads
                      else self.allocate(i))
            if reason:
                blocked_threads.setdefault(thread, i.id)
                self.blocked[str(i.id)] = reason
                self.waits[str(i.id)][reason] += 1
                continue
            # TileReg → Scheduler：Allocated 携带原请求 ID，允许该指令进入待派发状态。
            # 预留不发布 ready；Scheduler 仍须等待数据和同步依赖。
            self.send(core_name(i.core, 'scheduler'), Allocated(i.id, request.request), 'allocation')
            self.allocations.remove(request)
        # 4. 按确定性交付顺序仲裁访问；未 ready 的读请求可被绕过，读写端口彼此独立。
        for request in list(self.accesses):
            if request.write:
                assert request.version in self.active, request
            elif request.version not in self.ready:
                self.blocked[request.request] = f'tile_not_ready:{request.version}'
                continue
            shared = self.versions[request.version].pool.endswith('/shared')
            if shared:
                event = self.start_access(request)
                if event is None:
                    self.blocked[request.request] = 'shared_port_busy'
                    continue
                self.observe(request.instruction, stage=event)
                self.running.append((request, event['complete']))
            else:
                # 私有访问的服务成本已折入计算；本拍即可发送 AccessDone，但仍有消息往返延迟。
                self.respond(request)
            self.accesses.remove(request)

    def is_idle(self):
        return super().is_idle() and not (self.allocations or self.accesses or self.running
                                               or self.router_running or self.port_fences or self.reservations)

    def next_event(self):
        return min((end for _, end in self.running + self.router_running), default=None)

    def snapshot(self):
        return dict(super().snapshot(), calendar_receives={str(k): dict(pending=sorted(v['pending']),
                    committed=sorted(v['committed']), published=v['published']) for k, v in self.receives.items()},
                    write_reservations={str(k): v for k, v in self.reservations.items()},
                    port_fences=sorted(self.port_fences), allocations=[r.request for r in self.allocations],
                    accesses=[r.request for r in self.accesses],
                    running=[(r.request, end) for r, end in self.running], blocked=self.blocked.copy(),
                    usage=dict(self.usage), active=[list(k) for k in sorted(self.active, key=str)],
                    ready=[list(k) for k in sorted(self.ready, key=str)],
                    consumers={str(k): sorted(map(str, self.versions[k].consumers - self.done))
                               for k in sorted(self.active, key=str)})
