# wse-perf-sim

纯 Python WSE 性能仿真器，包含超标量 NPU 核、Calendar NoC（片上网络）和 HBM（高带宽内存）模型。每核的 NoC 端点是 SharedTileReg，SRAM 作为核内存储。支持 PyPTO capture 导入、显式通信计划、Decode 时序模拟和时间线导出。

代码按职责分层，核模型、NoC 和 HBM 位于 `models/` 下的平级目录。PTO capture 导入在 `frontend/`，报告和绘图在 `analysis/`。完整目录、依赖边界及旧路径对照见 [项目结构](docs/STRUCTURE.md)。

需要 Python 3.11 或更新版本。在仓库根目录可以直接运行以下命令；也可通过 `python3 -m pip install -e .` 安装，随后使用 `wse-perf-sim` 和 `wse-perf-trace` 命令。Decode 静态绘图另需 `python3 -m pip install -e '.[decode]'`。

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. python3 -S -m unittest discover -s tests -v
```

[WSE 参数与执行语义](docs/WSE.md) · [CommUnit 解耦接口](docs/COMMUNIT.md) · [历史通信接口验证](docs/COMMUNIT_VALIDATION.md) · [历史验证](docs/VALIDATION.md)

无通信计划时按原逻辑验证配置运行。统一使用 `from api import simulate` 和 `python -m cli`，原模块的迁移路径见结构文档的对照表。

**核模型与使用说明**

FlashAttention 示例读取 PyPTO 导出的指令流，按指定硬件配置和通信计划回放，并输出时序报告与流水图。[FA 回放命令](examples/decode/README.md)

Python 3.11+、仅标准库。公共回放入口读取静态 workload capture v2 / SSNPU v1（手写或 PyPTO 导出），采用独立组件、消息队列和周期驱动。每 Core 建立 Scheduler、ROB、TileReg、TLSU、Sync，每 PE 建立 Cube、Vector。Core 间资源独立，PE 数默认 4。参数用于逻辑验证，尚未校准硬件周期精度。

从仓库根目录执行，所有新产物写入仓内 `workspace/`：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. python3 -m cli \
  examples/matmul/capture --config config/logic-validation.json \
  --output workspace/ssnpu-components/matmul.json

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. python3 -m unittest discover -s tests -v

PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. python3 tests/replay_captures.py \
  examples/matmul/capture examples/fa/capture \
  --output-dir workspace/ssnpu-components/replay
```

公开 API 为 `from api import simulate`；`simulate(capture_dir, config=None)` 返回 JSON 可序列化报告，config 可为字典或 JSON 文件路径，异常类型为 ModelError。设置顶层 `message_log: true` 开启带发送/接收周期的消息日志。需要观察中间状态时，从 `simulator` 导入 `Simulator`，从 `frontend.capture` 导入 `TraceAdapter`，构建 `Simulator(TraceAdapter(path, config), config)` 并调用 snapshot；测试可通过 tick_order 指定组件调用顺序。

指令打点始终包含在报告的 `instruction_events` 中，也可导出为按周期排列的 JSONL，用于绘制流水图：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. python3 -m cli \
  examples/matmul/capture --output workspace/ssnpu-components/pipeline/matmul.json \
  --event-log workspace/ssnpu-components/pipeline/matmul.events.jsonl
```

每行包含 cycle、event、op_id、ir_op、core、pe、task、block、thread、control、component 和 execution_component。按 op_id 配对 issue / rob_complete，即得到指令从发射到 ROB 完成的区间；按 execution_component 分执行单元泳道。control=true 表示 barrier/task 控制节点，可按需过滤。API 直接读取 `report['instruction_events']` 或 `report['timeline']`。

打点包含 admitted、admission_received、allocation_request、allocated、issue、received、operands_ready、compute_start/end、writeback_start/end、complete、rob_complete、retire。admitted 是 ROB 消耗槽位并发出准入通知的周期，admission_received 是下一周期 Scheduler 收到通知的时间；控制节点没有这两个打点。complete 是执行单元完成并发送通知；rob_complete 是下一周期 ROB 接收完成通知；retire 是按线程顺序退休，可能晚于 rob_complete。消息日志的 message_log 开关与指令打点独立。Perfetto 保留准入打点，也兼容没有准入字段的旧日志。

打点日志可用独立脚本转换为原生 Perfetto protobuf，无需重跑仿真或安装第三方库：

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=. python3 -m analysis.perfetto \
  workspace/ssnpu-components/pipeline/matmul.events.jsonl \
  --output workspace/ssnpu-components/pipeline/matmul.pftrace \
  --cycle-ns 1
```

在 [Perfetto UI](https://ui.perfetto.dev/) 中选择 Open trace file 打开 .pftrace。原生输出通过 TrackDescriptor.parent_uuid 建立真实父子关系，Core、PE、执行单元和命令组可以折叠展开：

```text
Core 0
├─ PE 0
│  ├─ Cube
│  │  └─ In-flight commands
│  │     ├─ active 0
│  │     └─ active 1
│  └─ Vector
├─ TLSU
├─ ROB
└─ Scheduler
```

执行单元的 active 子轨道只覆盖 received→complete：从单元接收命令，到全部输出写回并发送完成通知，包含操作数访问、资源等待、计算和写回。ROB 通知及退休等待放到 Core 的独立 ROB 分组，Scheduler 的分配和发射打点另列，全部原始打点保留为瞬时事件。active 轨道重叠表示多条命令同时在单元内等待或推进；compute 阶段包含服务及结果延迟，不能当作纯资源占用。旧控制/view 日志没有 received，使用 issue→complete 并在详情中标明起点事件。

默认 1 cycle=1 ns 是用于查看的合成映射，不代表硬件频率；可用 --cycle-ns 调整，原生时间戳四舍五入到整纳秒，事件详情仍保留全部原始周期。重叠指令分轨避免非嵌套区间；缺失关键打点、重复事件或时序冲突会报错。Python 调用 `analysis.perfetto.to_pftrace(events, cycle_ns=1)` 返回可直接保存的 bytes。

为兼容旧工具，输出文件后缀为 .json 时仍导出 [Trace Event JSON](https://perfetto.dev/docs/getting-started/other-formats#chrome-json-format)，也可用 --format json/pftrace 显式指定。JSON 轨道保持平铺，完整的可折叠层级需要 .pftrace。JSON 的 ts/dur 使用微秒；旧 API `to_perfetto(events, cycle_ns=1)` 仍返回 JSON 字典。

Simulator 将指令派给 Cube、Vector、TLSU，各单元内部实现自己的工作量与延迟公式，没有独立 CostModel 或预生成的执行阶段。TileReg 直接管理容量、版本和访问端口。

每组件每周期运行一次 tick，本周期消息只在下一周期交付。PE 间轮转、PE 内最老就绪发射；消息和执行单元队列无限。输出在派发前原子预留，tile 容量与发射额度继续生效。执行单元可同时持有多条在途命令，共享 Vector 类别竞争同一 PE 资源；共享读、共享写、memory 各自竞争 Core 服务资源。统一计时为 `service=max(1,ceil(work/throughput))`、`service_end=start+service`、`result=service_end+latency`，组件消息延迟单独计入。

`parameters.rob_depth.value` 限制每线程已准入、尚未退休的指令数：正整数为有限容量，`null` 为无限，默认及旧配置缺省为 `null`。普通 tile 指令和 view 都占槽位；barrier/task 控制节点由 Sync 管理，不计窗口容量。ROB 每周期先处理完成和按序退休，再按程序顺序填补空位，准入和退休宽度均无限。完成但尚未退休的指令仍占槽位。Scheduler 收到 Admitted 后才检查依赖和申请输出，窗口内继续允许就绪指令绕过阻塞指令。

报告 `rob_windows[ROB组件名][线程身份]` 给出 capacity、occupancy、peak、full_cycles；最后一项统计每周期退休和补入后，窗口满且仍有指令待准入的周期数。正常完成时 occupancy 为 0。等待原因包含 waiting_admission；死锁快照的 windows 包含窗口内指令、retirement_head 和 next_admission。实验配置与无限、4、16 深度的 matmul/FA 回放产物放在上级 `workspace/rob-depth/`。

| 文件 | 职责 |
|---|---|
| api.py / simulator.py | API、整机装配、时钟推进、停滞诊断并调用报告汇总 |
| runtime/clock.py / protocol.py | 下一周期交付、确定性消息顺序、dataclass 消息合同 |
| runtime/component.py / service.py | 队列接口、局部观察、服务时钟 |
| models/ssnpu/scheduler.py | 依赖唤醒、输出预留、轮转发射 |
| models/ssnpu/rob.py / sync.py | 按线程窗口准入和退休、barrier 与 task |
| models/ssnpu/tile.py | local/shared 版本、容量、ready、访问端口与释放 |
| models/ssnpu/cube.py / vector.py / tlsu.py / execution.py | 操作数握手、计算或搬运、独立服务与写回 |
| models/noc/ | Calendar 路由、时隙编译、通信合同与网络执行 |
| models/memory/hbm.py | 全片 HBM 带宽服务 |
| frontend/capture.py / communication.py | capture 输入校验、Tile 与通信计划绑定 |
| records.py / tiles.py | 各层共用的指令/版本记录、大小计算 |
| config.py | 参数及配置校验；指令工作量和延迟在对应执行单元内计算 |
| analysis/report.py | 只读汇总组件状态、时间线和统计 |
| analysis/perfetto.py / pipeline.py | 指令和资源时间线导出、泳道图 |
| cli.py | CLI |

报告包含总周期、最后指令完成周期、Core/PE 完成时间、发射/访问/计算/写回/完成/退休时间线、组件状态、服务统计、存储峰值及分配释放事件、等待原因、配置与输入 SHA256。总周期包含控制节点、退休、释放和消息收尾。完成广播和 tile 释放不等待退休。停滞异常包含各组件的队列、依赖、请求、占用版本和等待条件。

路径表默认将 DDR/L1/L2 映射到 memory；路径项可设为 `{"engine":"memory","throughput":512,"latency":4}`。各计算类别支持 dtype_overrides。配置记录数值、单位、作用域、含义和来源。

[DESIGN.md](DESIGN.md) 说明组件协议、存储生命周期、可手算周期和 load→matmul→store 的代码阅读顺序。[examples/](examples/README.md) 的 capture、源码和历史报告保留原样，旧报告周期仅供比较。

任意节点组的 AllGather 可用 `frontend.communication.allgather_group` 自动生成多树路由和发送窗口，再交给 `simulate(..., communication_plan=plan)`。用法见 [WSE 自动规划](docs/WSE.md#自动规划)。

[从 gfrun 捕获 PTO tile trace](docs/gfrun-tile-trace.md) 记录同一 ELF 驱动两套性能模型的备选路线、标准紧凑格式需要补充的运行时信息，以及当前目标编译工具链尚未就绪的限制。

[SSM 完整 benchmark 结果](docs/benchmarks/ssm-native-results.md) 和 [首轮参数校准](docs/benchmarks/ssm-calibration.md) 包含单 PE 固定用例的周期、可选校准配置与复现命令。`benchmarks/ssm_baseline.py --timeout 0` 支持无时间限制运行；`benchmarks/calibrate_ssm.py` 可基于已保存的 SSM 结果重做拟合。

[核与 NoC 独立运行](docs/DECOUPLING.md) 说明 simulate_core、假通信后端、假核端点和联合运行的职责边界。
