# SSNPU 组件协议与建模边界

## 输入与状态归属

`api.simulate(capture_dir, config=None)` 保持原接口；配置仍可使用字典或 JSON 路径。`TraceAdapter` 读取 capture v2 / SSNPU v1，校验线程身份、顺序、数据依赖、同步、barrier 参与者和版本生命周期。`order_dependencies` 只确定线程年龄与退休顺序，不转成执行完成依赖。适配器构建后，Instruction / Version 元数据只读；`operands` 保留输入操作数的原始顺序和解析别名后的版本。

每 Core 建立 Scheduler、ROB、TileReg、TLSU、Sync、CommPort，每 PE 建立 Cube、Vector。TileReg 直接持有容量和版本状态，不再另包一层存储管理器。组件持有自己的队列和动态状态；没有组件引用其他组件实例。Simulator 只将指令路由给对应单元，不生成执行阶段。Cube、Vector、TLSU 内部的 `start()` 直接解析指令并计算工作量和延迟；公共 `Service` 只记录服务时钟与统计，`ExecutionUnit` 只复用操作数握手、计算队列和结果返回逻辑，不调度其他组件。

| 组件 | 独占的动态状态 |
|---|---|
| Scheduler | 依赖完成集合、待发射窗口、分配请求状态、PE 轮转与发射额度 |
| ROB | 按原始线程顺序预加载的条目、准入游标、窗口占用、完成状态、退休头 |
| TileReg | local/shared 容量、版本 ready、完整消费者完成集合、分配等待、共享读写端口 |
| Cube / Vector | 多条在途命令、有序操作数访问、计算服务、结果延迟及写回等待 |
| TLSU | Core 内存服务与路径 cost、load 写回、store 输入读取及内存写入完成 |
| Sync | 控制节点依赖、barrier 延迟、task 放行 |

ROB 按线程限制已准入但未退休的指令数（rob_depth 为正整数或 null，缺省无限）；消息和执行单元队列无限；PE/Core 发射宽度、计算与搬运吞吐、tile 容量和同步限制继续生效。多个 Core 的资源独立，完成广播可跨 Core 传递 task / barrier 依赖；不支持跨 Core tile 数据通信。

## 时钟与消息

所有组件提供 `receive(port, message)`、`tick(cycle)`、`drain_outputs()`、`is_idle()`、`snapshot()`。`receive` 入队，`tick` 处理输入及内部服务，`drain_outputs` 取走输出；状态查询不修改任何队列或资源。

一个周期严格分为三步：

1. `Clock.deliver(cycle)` 把上周期发出的消息交给输入队列。
2. 每个组件恰好调用一次 `tick(cycle)`。
3. `Clock.collect(cycle)` 收集输出，安排在 `cycle + 1` 交付。

没有同周期消息回环。Clock 按源组件名称及源端 FIFO 顺序收集消息，形成确定性到达顺序；改变 tick 顺序不会改变仲裁和报告。端口使用 commands、allocation、access、completion、consumption 等名字。`protocol.py` 的 frozen dataclass 定义消息，Envelope 记录源、目的、端口和发送/接收周期；Access 还带版本、请求身份、方向、字节数和 dtype。

| 消息 | 发送方 → 接收方 | 含义 |
|---|---|---|
| Admitted | ROB → Scheduler | 已占线程窗口槽位，下一周期允许检查依赖及预留输出 |
| Allocate / Allocated | Scheduler ↔ TileReg | 原子保留全部输出，等待期间不重复请求 |
| Dispatch | Scheduler → Cube / Vector / TLSU / TileReg | 已分配命令；view 交给 TileReg |
| Issued | Scheduler / Sync → ROB | 发射通知 |
| Access / AccessDone | 执行单元 ↔ TileReg | 按操作数顺序访问输入，或逐一写回输出 |
| Complete / Completed | 执行单元 / TileReg / Sync → ROB → 各 Scheduler / Sync | 最终完成与依赖广播 |
| Consumed | 执行单元 → TileReg | 指令已完成，输入不再使用，输出可检查释放 |

ROB 在加载时按 `_identity` 和 `age` 建立线程条目。每周期先处理完成消息、按序退休，再按程序顺序填补空位；退休和准入宽度无限。准入占槽位，按序退休归还，完成但未退休仍占容量。view 计入窗口；控制节点不参与准入，由 Sync 管理。无限配置也通过同一 Admitted 消息路径。发射可以乱序，完成立即广播，退休只从各线程头推进。控制节点单独跟踪退休，不用 PE 的线程位置模拟 barrier。零延迟控制节点同周期发送 Issued 和 Complete，仍经历消息交付。依赖唤醒与 TileReg 释放均不等待退休。

## 发射、服务与完成

WSE 配置的计算吞吐按整核计量：FP16 Cube 为 4096 MAC/cycle/核，Vector 与转换路径以 256 B/cycle/核为预算。当前采用固定等分模型，每个 PE 得到整核预算的 `1/pe_count`，空闲份额不借给其他 PE；这是建模假设。Vector 按操作数据类型把字节预算换算为元素吞吐，exp/reduce 还受配置中的合成操作吞吐约束。通用 `default_config()` 保留原来的每 PE 合成参数。上述预算不包含真实阵列的小矩阵利用率校准。

Scheduler 从 waiting_admission 开始，收到 Admitted 后进入 waiting_allocation；按线程程序顺序只发送一次输出分配请求，不等待数据输入 ready。任务依赖和 barrier 等同步边仍限制跨阶段预留；同步受阻的指令也阻止本线程后续申请。请求可以同拍连续发送，无须等前一条 Allocated 往返。TileReg 按线程顺序原子分配全部输出：受阻请求留在队列，其线程后续请求等待，其他线程继续推进，同拍可连续满足多个请求。

收到 Allocated 后，Scheduler 仍检查数据和同步依赖，再按 PE 间轮转、PE 内最老就绪顺序发射。预留容量在等待依赖或发射时继续占用；年轻独立指令完成预留后可绕过等待数据的老指令。成功派发才消耗 PE/Core 额度，资源繁忙由执行组件排队处理。view 不申请新容量，派发仍等待真实输入。Comm submit 继续预留 wait 的接收输出，同拍到达的 wait 分配请求也不会重复计费。

Cube / Vector / TLSU 顺序请求每个 tile 输入，包括私有输入；无 tile 输入的命令可在接收周期进入计算。等待输入或结果的旧命令不阻塞其他就绪命令。就绪命令按 `(task, age, id)` 仲裁，各组件独立推进自己的服务。exp、cast、reduce 与逐元素计算共享该 PE 的 Vector 资源。

统一 cost 公式保持为：

```text
service = max(1, ceil(work / throughput))
service_end = start + service
result = service_end + latency
```

服务区间为半开区间；下一条计算可在 service_end 开始，上一条结果仍在等待 latency 或写回。消息周期不混入 cost。SharedTileReg 每个输入和输出分别按字节数竞争 Core 的 shared_read / shared_write 端口，两种方向使用独立资源。私有访问的额外服务 cost 包含在原有计算假设中，但请求和响应各占一个消息周期。输入完成后才计算，计算结果产生后才请求写入；全部输出写回响应到达后发送 Complete / Consumed。store 在内存写入结果产生后完成，不等待不存在的 tile 输出。

工作量和延迟由执行单元直接根据指令计算：Cube 的 matmul(_acc) 使用 M×N×K MAC；Vector 的逐元素使用有效输出元素数；归约使用有效行数×(有效宽度−1)；TLSU 的 memory 使用 memory_accesses 范围×dtype 字节数。路径支持 DDR/L1/L2 与 throughput / latency 覆盖；不模拟 cache。view 不分配新存储、不占执行资源和 tile 发射额度，但依赖放行和完成仍经过消息。

## 存储生命周期

TileReg 是唯一可修改版本 ready、容量和消费者完成状态的组件。版本的完整消费者在加载时预先确定；普通版本在生产完成且所有消费者完成后释放，无消费者的输出在生产完成消息到达时释放。没有活跃读取请求并不等于可以释放。shared last-use 必须覆盖参与者并等待所有其他消费者；只有 reuse 的共享版本作为 live-out 保留。

Local 和 Shared 输出统一以规范化后的动态版本为独立分配身份：Local 为 `(storage_id, version)`，Shared 为 `shared_version`。同一 storage 的新旧版本可同时占用完整容量，适配器不因 storage_id 或 shared handle 相同增加覆盖依赖，也不要求不同版本的跨线程生产者建立显式顺序。重复版本生产者仍报错，已有显式依赖、RAW 和同步依赖保留。读旧写新的输入旧版本和输出新版本分别计费；容量不足必须等待，不能扣除仍在使用或 retained 的旧版本。view/reshape 链规范化到源版本，不重复分配。多输出预留保持原子性，跨 Local/Shared 容量池也须全部检查成功后才分配。

Local 的释放规则是粗粒度近似：生产者和全部消费者完成后回收，不等待 ROB 退休，也没有完整对齐 SSM 的退休和提前释放机制。issue #6 的 `A=full; B=relu(A); C=full; B=add(B,C)`，每 tile 为 1 KiB 时最后一步需要旧 B、C、新 B 三份空间；3 KiB 可完成，2 KiB 的容量不足不能通过隐式原地覆盖掩盖。

Shared 保留发布、消费和释放规则：last-use 允许在生产者和全部消费者完成后回收；reuse 或没有 last-use 的版本继续作为 live-out 保留，即使同一 storage 已产生新版本也不释放。因此保留版本可能让后续分配持续等待并触发容量死锁。没有固定位置覆盖、容量转移和 `superseded_by` 事件。适配器继续校验 Shared 消费组、last-use / reuse、别名环、缺失生产者和图环。

统一独立分配接近 SSM 容量充足时的新版本分配行为，但不实现 Shared 容量不足时的原地复用回退，也不实现新 PGI 提交时回收被替换映射的机制。NoC 路由和传输逻辑及现有通信绑定限制不变。

## 报告与停滞

`total_cycles` 是从周期 0 开始，所有指令和控制节点完成、ROB 退休、消费释放消息与完成广播全部处理完毕的最后周期编号。另列 `last_instruction_complete_cycle`（不含控制节点）和 `last_node_complete_cycle`（含控制节点），区分执行结束与收尾。

时间线保留 issue、complete、stages，增加 queued、admitted、admission_received、allocation_request、allocated、received、operands_ready、compute_start/end、writeback_start/end、retire。accesses 列出每个操作数请求与响应的周期，transitions 列出各组件状态转换。component 表示最终所在组件（退休时为 ROB），execution_component 保留执行单元；view 和控制节点没有不存在的计算/分配阶段。

ROB 准入记录 admitted，Scheduler 下一拍记录 admission_received；窗口外指令只能等待准入，不能申请输出或派发。ROB 收到 Complete 时记录 rob_complete，早于或等于 retire。报告的 instruction_events 将各组件实际记录的里程碑打点按周期输出，CLI 的 --event-log 可另存 JSONL。每行携带指令身份、执行单元与记录事件的组件，画流水图可直接配对 issue / rob_complete；细分阶段可使用 compute_start/end 与 writeback_start/end。消息延迟固定为一周期，因此 rob_complete=complete+1，而退休可因同线程更老指令未完成而延后。

设置配置顶层 `"message_log": true` 输出 messages，默认关闭。每条日志包含 type、instruction、source、destination、port、sent、received，以及该消息的请求/版本等字段。资源利用率只计算服务时间，不包含消息延迟。wait_reason_sets 统计等待指令周期：waiting_prior_allocation 指明前序分配阻塞，capacity 指明容量池，dependency 表示等待数据，synchronization 表示等待同步，issue_width 仅在已预留且依赖就绪时表示等待发射额度。allocation_pending 包含请求/应答在途及 TileReg 排队时间，具体阻塞查看 TileReg 的 blocked；Scheduler 快照的 reasons 同时列出本地等待原因。TileReg 容量等待和 Scheduler 等待可重叠，不可直接相加；内部计算资源等待另列。

没有消息在途、没有内部未来服务结果且一个周期没有进展时，Simulator 抛出 ModelError，包含所有组件快照：分配/访问请求、队列、依赖、在途结果、占用版本、消费者及具体容量阻塞。`Simulator.snapshot()` 也可主动调用。完成报告包含组件最终快照。ROB 的 windows 显示窗口内指令、退休头 retirement_head 和下一条待准入 next_admission。报告 rob_windows 按组件及线程列出容量 capacity、当前 occupancy、峰值 peak 和 full_cycles；full_cycles 在每拍退休、补入后统计窗口满且仍有待准入指令的周期数。

## 从 load → matmul → store 阅读代码

先读 `api.py` 和 `simulator.py` 的组装与时钟循环，再按下列消息流读组件。完整目录见 [项目结构](docs/STRUCTURE.md)。假设 B 已就绪，load 产生 A，matmul 产生 C，store 写回 DDR：

```text
ROB --Admitted(load)--> Scheduler
Scheduler --Allocate(A)--> TileReg --Allocated--> Scheduler
Scheduler [matmul 已准入] --Allocate(C)--> TileReg --Allocated--> Scheduler
Scheduler [store 已准入] --Allocate(无 tile 输出)--> TileReg --Allocated--> Scheduler
Scheduler --Issued(load)--> ROB
Scheduler --Dispatch(load)--> TLSU
TLSU [memory service + latency] --Access(write A)--> TileReg
TileReg --AccessDone--> TLSU --Complete(load)--> ROB --Completed--> Scheduler/Sync
                          \--Consumed(load)--> TileReg
Scheduler [matmul 依赖就绪] --Dispatch(matmul)--> Cube
Cube --Access(read A)--> TileReg --AccessDone--> Cube
Cube --Access(read B)--> TileReg --AccessDone--> Cube
Cube [cube service + latency] --Access(write C)--> TileReg --AccessDone--> Cube
Cube --Complete(matmul)--> ROB --Completed--> Scheduler/Sync
     \--Consumed(matmul)--> TileReg [检查 A/B 释放]
Scheduler [store 依赖就绪] --Dispatch(store)--> TLSU --Access(read C)--> TileReg --AccessDone--> TLSU
TLSU [memory write service + latency] --Complete(store)--> ROB
                                    \--Consumed(store)--> TileReg [检查 C 释放]
```

每条箭头固定占一周期。依次阅读 `models/ssnpu/` 下的 `scheduler.py`、`tile.py`、`tlsu.py` / `execution.py`、`cube.py`、`rob.py`；再读 `sync.py` 理解尾部 barrier 与 task 完成。Vector 使用相同的小型命令结构，服务资源为共享 Vector。

可手算基准：4 元素 full，throughput=4、latency=2、私有输出。周期 0 准入，1 Scheduler 收到通知并请求分配，2 分配，3 派发，4 接收并计算，5 服务结束，7 结果产生并请求写入，8 写入，9 响应并完成，10 ROB 接收完成并退休、TileReg 处理消费，11 完成广播交付。单任务完成节点于 11 完成，12 退休，13 最后广播交付，所以 total_cycles=13。该期望由协议推导，不沿用旧周期数。

## 验证和边界

`tests/test_components.py` 独立驱动各组件，验证下一周期可见、ROB 乱序完成/按序退休、一次分配、原子容量等待、有序操作数、就绪绕过、服务与结果重叠、共享端口、完整消费者释放、store 完成和 Sync 空阶段。反转及随机打乱 tick 顺序后，报告和消息日志必须逐项一致。`test_rob_window.py` 验证容量、年轻指令完成仍占槽位、窗口内绕过、view/control 计数、配置兼容、快照与有限模式调用顺序；`test_model.py` 覆盖完整数据流和异常；回放审计核查指令唯一性、依赖、额度、服务区间、访问握手、准入/退休及窗口占用独立重建、分配/释放唯一性和容量上下界。

合成配置回放中，matmul 为 168 条 / 7042 周期，FA 为 320 条 / 2183 周期；无限、4、16 三种深度的总周期在这两份 capture 上相同。matmul 的最大单线程窗口峰值分别为 24、4、16，FA 为 43、4、16，所有窗口最终占用为 0。新准入握手使无限模式也比旧模型多一拍启动延迟，不要求旧周期数完全一致。examples 的历史归档不改写。配置、完整报告、事件日志、Perfetto 和汇总保存在上级 workspace/rob-depth；测试临时 capture 仍写入 workspace/ssnpu-components/tests。

核时序模型按配置估算服务速率和延迟。缓存驻留、bank 冲突和单条指令内部多阶段重叠未建模。

## WSE 通信接口

核侧 `CommPort` 把 `comm.submit` / `comm.wait` 和 Tile 访问转换为请求、数据访问、完成令牌。WSE 侧 `CommUnit` 只接收通用协议和独立安装配置，负责接纳、时隙、传输与发布。详细合同、故障和收尾语义见 [COMMUNIT.md](docs/COMMUNIT.md)。
