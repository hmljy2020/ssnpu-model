# 核与 NoC 的独立运行

SSNPU 读取静态 tile trace JSON/JSONL。capture v2 / SSNPU v1 的执行字段仍被校验；捕获模式、入口函数、pto_op 和 function 是可选追踪信息。手写输入和解释器生成的输入使用同一入口。

## 核独立运行

```python
from api import simulate_core
report = simulate_core('examples/ooo/capture')
assert report['total_cycles'] == 23
```

默认使用原核配置。独立核模式要求配置不包含 HBM；需要 HBM 时使用联合入口。公共 simulate_core 接受 trace 中已有的通信指令；通信后端通过 comm_backend 注入。通信指令没有后端会明确报错。若使用现有外部 communication_plan 生成通信节点，可以先构造 TraceAdapter，再调用 Simulator(trace, config, core_only=True, comm_backend=peer)。

models/ssnpu/system.py 仅负责核组件装配。核只通过 runtime/communication.py 中的消息与后端交互。后端使用 communit 队列名，继承 Component 的队列与事件方法；如有异步网络事件，可提供 advance(cycle)，返回 (destination, message, exact_time_ns)。普通测试后端只需 tick，无需实现网络推进。

examples/communication/fake_communit.py 提供脚本后端。脚本依次产生 DataCommand，在收到对应 DataReply 后继续；完成前应经过必要的准备、读写和发布。test_decoupling.py 给出了真实核连接假后端的用例，包括缺失后端报错和消费者唤醒。

## NoC 独立运行

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. python3 -S examples/communication/driver.py \
  --output workspace/communit-driver/report.json
```

驱动使用 ByteClient 作为假核，直接提交公共 CommRequest，并响应读写、准备和发布。真实 CommUnit 执行网络行为。该路径无需 capture、核调度器或 TileReg。源数据使用确定的字节模式，驱动检查每个目标的实际字节。

## 联合运行与职责

原 from api import simulate 及 python -m cli 调用方式保留。提供 communication_plan 时保留现有联合配置选择行为；要比较核性能，显式传入同一配置。

- 核：指令依赖、发射、ROB、tile 容量与版本、存储服务、发布回收。
- NoC：通信匹配、准入、Calendar、路由、网络资源及传输完成。
- 公共接口：请求、接管、数据命令、响应和终态消息，不包含模型对象引用。
- 联合装配：加载配置、连接组件和按需创建 HBM。
- runtime/runner.py：周期推进、下一拍消息交付、外部事件交付和停滞诊断。具有定时动作的组件必须提供 next_event，避免把等待时间误判为死锁。

双方通过受控句柄和范围读写。WriteDone 表示写入可见，Published 表示目标可供消费者使用，Completion 表示本地事务成功或故障。假组件与真实组件遵守相同消息合同。

本轮保留现有 AllGather 通信实现；issue #4 新讨论的一对多／多对一、二维布局和流式传输是下一轮协议升级。公共接口中的现有字段暂不改变。
