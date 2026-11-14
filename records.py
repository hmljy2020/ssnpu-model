"""Capture instruction and storage version records."""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class Instruction:
    """只读使用的指令元数据；动态执行状态存放在各组件中。

    deps/sync 是必须完成的前驱；age 用于仲裁/退休，不自动生成相邻指令完成依赖。
    inputs/outputs 是去重后的存储版本集合，operands 保留实际操作数的访问顺序。
    """
    id: object
    core: int
    pe: int
    task: int
    block: int
    age: int
    raw: dict
    deps: set = field(default_factory=set)
    sync: set = field(default_factory=set)
    inputs: set = field(default_factory=set)
    outputs: set = field(default_factory=set)
    alias: bool = False
    control: bool = False
    operands: list = field(default_factory=list)


@dataclass
class Version:
    """key 标识规范化后的动态版本，pool 决定独立计费的容量域。

    consumers 包含完整 trace 的消费者；retained 为真时消费结束仍保留载荷。
    """
    key: tuple
    obj: dict
    producer: object
    pool: str
    size: int
    consumers: set = field(default_factory=set)
    retained: bool = False
