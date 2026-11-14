"""组件间的消息合同；消息只携带身份和元数据，不搬运 tile 数值。

主路径：ROB --Admitted--> Scheduler --Allocate--> TileReg --Allocated--> Scheduler
        Scheduler --Dispatch--> Cube / Vector / TLSU --Access--> TileReg
        TileReg --AccessDone--> 执行单元 --Complete--> ROB
        ROB --Completed--> 所有 Scheduler / Sync
执行单元同时向 TileReg 发送 Consumed，驱动版本回收。
所有 instruction 字段都是指令 ID；request 用于匹配一次请求与响应。
发送方、目的组件、端口和发送/接收周期由 Clock 包装到 Envelope。
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class Admitted:
    """ROB → Scheduler：已占用线程窗口槽位，下一拍可按序申请输出预留。"""
    instruction: object


@dataclass(frozen=True)
class Allocate:
    """Scheduler → TileReg：按线程顺序发送，一次性预留全部输出空间。"""
    instruction: object
    request: str


@dataclass(frozen=True)
class Allocated:
    """TileReg → Scheduler：预留成功；输出尚未写入，也尚未 ready。"""
    instruction: object
    request: str


@dataclass(frozen=True)
class Dispatch:
    """Scheduler → 执行单元（view 发给 TileReg）：接收并推进该指令。"""
    instruction: object


@dataclass(frozen=True)
class Issued:
    """Scheduler / Sync → ROB：登记该指令或控制节点已经发射。"""
    instruction: object


@dataclass(frozen=True)
class Access:
    """执行单元 → TileReg：访问指定版本；write 区分读写，work 单位为字节。"""
    instruction: object
    request: str
    version: tuple
    write: bool
    work: int
    dtype: str


@dataclass(frozen=True)
class AccessDone:
    """TileReg → 原执行单元：该 request 的读写已完成，可以推进操作数/输出游标。"""
    instruction: object
    request: str


@dataclass(frozen=True)
class Complete:
    """执行单元 / TileReg(view) / Sync → ROB：操作已完成，请登记并广播。"""
    instruction: object


@dataclass(frozen=True)
class Completed:
    """ROB → Scheduler / Sync：依赖节点已完成；无需等待该节点按序退休。"""
    instruction: object


@dataclass(frozen=True)
class Consumed:
    """执行单元 → TileReg：整条指令完成，所有输入已消费、输出已写回。"""
    instruction: object


@dataclass(frozen=True)
class Envelope:
    """Clock 的路由与时序封装；received 固定等于 sent + 1。"""
    source: str
    destination: str
    port: str
    message: object
    sent: int
    received: int


def core_name(core, unit):
    return f'core:{core}/{unit}'


def unit_name(i):
    # 只决定命令目的组件；实际工作量和资源占用由该组件内部计算。
    op = i.raw['ir_op'].removeprefix('tile.')
    if i.alias:
        return core_name(i.core, 'tile')
    if op.startswith('comm.'):
        return core_name(i.core, 'comm')
    if op in ('load', 'store', 'ssnpu.load_into', 'memory.copy'):
        return core_name(i.core, 'tlsu')
    return core_name(i.core, f'pe:{i.pe}/' + ('cube' if op in ('matmul', 'matmul_acc') else 'vector'))


@dataclass(frozen=True)
class ReceivePrepare:
    instruction: object
    version: tuple
    expected: tuple  # (source, offset, nbytes)


@dataclass(frozen=True)
class PortFence:
    instruction: object


@dataclass(frozen=True)
class WriteReservations:
    instruction: object
    entries: tuple  # (source, start cycle, service cycles, result latency)


@dataclass(frozen=True)
class RouterWrite:
    instruction: object
    version: tuple
    source: int
    offset: int
    work: int


@dataclass(frozen=True)
class PublishReceive:
    instruction: object


@dataclass(frozen=True)
class TransferReply:
    instruction: object
    event: str
    detail: object = None


@dataclass(frozen=True)
class HBMRequest:
    instruction: object
    core: int
    work: int
    direction: str
    first_stage: bool


@dataclass(frozen=True)
class HBMDone:
    instruction: object
