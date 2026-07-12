# Backlog(范围纪律:W10 前只记不做)

- DAG 依赖边调度(当前仅 fan-out/fan-in);SubtaskDef 预留 dependsOnSeq
- Kafka UI 容器(如 provectuslabs/kafka-ui,可视化 topic/consumer group,arm64 兼容)
- Worker RUNNING 状态上报(心跳);W9 可选 Zookeeper 注册/选主
- 为 maven-compiler-plugin 开启 <parameters>true</parameters>(否则新增 @PathVariable/@RequestParam 不写显式名会在 Spring 6.1 运行时失败)
- **【P1,Task 7 实盘复现发现的真实缺陷 —— 已于 W3 修复】`DlqRecoveryService.replay()` 状态迁移缺口**:
  重投时把子任务直接置为 `PENDING`(而非 `DISPATCHED`)后就发消息到 `AGENTFLOW_SUBTASK`,
  但 `ResultHandleService.handle()` 要求 CAS 源状态必须是 `DISPATCHED` 才会推进(`transitionSubtask(DISPATCHED→终态)`)。
  于是重投后无论 worker 处理成功还是再次失败,回传结果都会因为 CAS 源状态不匹配(实际是 PENDING)而被
  静默丢弃("子任务状态已变更,忽略重复结果"),任务永久卡在 RUNNING,子任务永久卡在 PENDING,
  且 `TimeoutSweepService.findStuckDispatched` 只扫 `DISPATCHED` 行,也捞不到这个 PENDING 的"孤儿"。
  2026-07-12 用真实空字符串输入触发 DLQ→replay 全链路时复现:`subtask.id=495` 重投后确实再次经历
  RETRY_5S→30S→5M→DLQ 完整阶梯并二次进 DLQ(worker 日志可见),但 `task.id=444` 的 `status` 停留在
  `RUNNING`、`subtask_failed=0` 再未变化(直接查库确认)。
  **修复(同日,W3):** `replay()` 重投前把子任务置为 `DISPATCHED`(仿 `SubtaskDispatcher.dispatch` 的
  "发送即视为在途"语义),而不是放宽 `ResultHandleService` 的 CAS 源状态——保持 CAS 语义单一、状态机
  不为这一个调用方特例放宽。`DlqRecoveryServiceTest` 同步补上 `assertEquals("DISPATCHED", ...)` 断言
  (原先只断言即时重置状态、未覆盖到"重投结果回传能否被正常 CAS 接受"这一环,是这个缺口能潜伏到实盘演练
  才暴露的原因)。

## 自检(W1–2 里程碑,对照 `docs/interview-signals.md` 验收清单)

1. **为什么状态迁移用数据库 CAS 而不是 `SELECT 再 UPDATE` 或分布式锁?**
   `SELECT` 再 `UPDATE` 在并发下存在"读到旧值、两个线程都认为自己可以转移状态"的竞态窗口(经典 check-then-act),需要额外加锁才能保证正确性;分布式锁(如 Redisson)能解决并发但引入了锁服务依赖、持有时长、死锁/续期等运维复杂度,对单行状态迁移这种场景是"杀鸡用牛刀"。数据库 CAS(`UPDATE ... WHERE id=? AND status=?`,判断 `affected rows == 1`)把"读+判断+写"压缩成一条原子 SQL,利用行锁天然保证互斥,无需额外基础设施,且失败方(`rowsAffected==0`)可以直接感知自己"抢输了"并放弃后续动作,语义清晰、成本最低。本项目中 `TaskMapper.casStatus` / `SubtaskMapper.casStatus` 均采用此模式,由 `TaskStateMachine.transitionTask/transitionSubtask` 统一收口,先校验状态机合法迁移表,再执行 CAS。

2. **进度为什么用计数列而不是 `COUNT(*)`?并发下谁触发聚合?**
   `COUNT(*)` 需要每次全表扫描 subtask 明细来现算进度,在高并发结果回传下代价高且存在"扫描时刻"与"实际更新时刻"不一致的脏读风险;而 `subtask_done`/`subtask_failed` 是 `task` 表上的原子自增列(`UPDATE task SET subtask_done = subtask_done + 1 WHERE id=?`),每条结果消息只需一次原子 UPDATE 即可推进进度,读进度是 O(1) 的单行读,不随子任务规模退化。
   并发下"谁触发聚合"的关键不在计数本身,而在于聚合动作前有一次 `transitionTask(RUNNING → 终态)` 的 CAS:多条结果消息并发到达、都算出 `done+failed==total` 并推导出同一个终态时,只有第一个执行 CAS 更新成功的线程(`rowsAffected==1`)会真正执行 `aggregate(...)` 写回聚合结果,其余线程 CAS 失败(行已不再是 RUNNING)后直接放弃,从而保证聚合只触发一次。此外 `ResultHandleService.handle()` 在自增计数前还有一层子任务级 CAS(`transitionSubtask(DISPATCHED → 终态)`),用于防止同一条结果消息被重复消费导致计数被多算。

3. **消息发送失败时子任务停在什么状态?谁来救?**
   停在 `PENDING`(`SubtaskDispatcher.dispatch` 中 `kafkaTemplate.send(...).get()` 抛异常时,只记录日志、不迁移状态,子任务保持 PENDING,不会误标记为 DISPATCHED)。当前 W1–2 骨架没有主动补救机制,由 W3–4 里程碑加入的"超时扫描/兜底重投"任务负责发现长期停留在 PENDING 的子任务并重新分发。

4. **部分子任务失败时任务的最终状态如何定义?为什么不是简单 FAILED?**
   定义为 `PARTIAL_FAILED`:当 `subtaskDone + subtaskFailed == subtaskTotal` 且 `subtaskFailed > 0` 且 `subtaskDone > 0`(即成功和失败都存在)时判定为 `PARTIAL_FAILED`;若 `subtaskFailed == 0` 则 `COMPLETED`;若 `subtaskDone == 0`(全部失败)则 `FAILED`。不用简单 `FAILED` 覆盖部分失败场景,是因为把"3 个子任务成功 2 个失败 1 个"和"全部失败"混为一谈会丢失有价值信息——调用方仍需要拿到已成功子任务的输出用于下游消费或展示,而不是因为一个子任务失败就丢弃全部结果。`PARTIAL_FAILED` 保留成功输出、暴露失败详情(`subtasks[].error`),对应面经中"最终状态如何定义"的追问;W3–4 会在此基础上补齐副作用补偿与精确重放(仅重放失败的子任务,而非整任务重跑)。

## W3–4 里程碑交付清单(Task 1–7,已通过 `scripts/fault-drill.sh` + 手工演练验收)

- [x] `@Transactional` 崩溃恢复修复(Task 1):`ResultHandleService.handle()` 补齐事务边界,避免"DB 已提交、Kafka offset 未提交"或反之的状态撕裂
- [x] 幂等去重(Task 2):`IdempotencyGuard`(Redis)按 `subtaskUuid+inputJson` 生成 key,仅在业务处理成功后 `markProcessed`
- [x] Kafka 自研阶梯延迟重试(Task 3):`RETRY_5S → RETRY_30S → RETRY_5M`,`RetryRouter` 按 attempt 路由,`RetryDelayListener` 用 header 携带 `attempt`/`not-before` 并 `Thread.sleep` 到点后重投主 topic
- [x] 死信队列(Task 3):重试耗尽(attempt≥3)进 `AGENTFLOW_DLQ` 并回传失败结果
- [x] 超时兜底扫描(Task 4):`TimeoutSweepService` 定时扫描卡在 `DISPATCHED` 超过 `stuck-seconds` 的子任务,未超 `MAX_REDISPATCH(3)` 重投,超过则落定 `FAILED`
- [x] `TaskFinalizer` DRY 抽取(Task 4):`ResultHandleService` 与 `TimeoutSweepService` 共用同一套终态判定 + 聚合逻辑
- [x] DLQ 恢复(Task 5):`POST /api/v1/dlq/replay/{subtaskId}` 重置子任务 FAILED→DISPATCHED、任务失败计数回退、任务复活为 RUNNING 后重投(注:曾误置为 PENDING 导致结果回传闭环断裂,该 P1 已于 W3 修复,见上方 backlog 条目)
- [x] 优雅停机 + 多 Worker 水平扩展(Task 6):`server.shutdown=graceful` + `spring.kafka.listener.immediate-stop=false`,同消费组多实例自动 rebalance
- [x] 端到端容错演练 + 里程碑验收(Task 7,本轮):`scripts/fault-drill.sh` + 三场景手工演练(见 `.superpowers/sdd/task-7-report.md`)

## 自检(W3–4 里程碑,`docs/backlog.md` 追加,对应 `task-7-brief.md` 自检题)

5. **幂等键为何只在成功后置?与合法重试如何不冲突?**
   `IdempotencyGuard.markProcessed(key)` 只在 `EchoProcessor.process()` 正常返回之后才调用(`SubtaskListener.onMessage` 里 `process()` 与 `markProcessed()` 之间没有异常),也就是"业务真正做完"才落幂等标记。如果处理过程中抛异常,幂等键不会被写入,子任务会正常进入 `RetryRouter` 走阶梯重试——重试消息本质是"同一个业务动作的重新尝试",而不是"重复消费同一条已完成的结果",所以合法重试必须能再次进入 `process()`;若在处理前就置位(如收到消息即 mark),会把"还没成功但已经尝试过"误判为"已完成",导致重试永远被幂等短路、失败子任务永远卡住无法真正重跑。幂等真正要挡的是"同一条已成功处理的消息因 rebalance/未提交 offset 被重复投递"这种场景,而不是"业务失败后的主动重试"。

6. **Kafka 无原生延迟,阶梯延迟怎么实现?为何 `max.poll.interval.ms` 要调大?pause/seek 方案相比 sleep 的取舍?**
   用三个独立 topic(`AGENTFLOW_RETRY_5S/30S/5M`)模拟延迟档位:失败时 `RetryRouter` 把消息连同 `x-retry-attempt`(当前重试次数)、`x-not-before`(到期时间戳)两个 header 一起发到对应档位 topic;`RetryDelayListener` 消费到消息后按 `not-before - now` 计算还需等待多久,`Thread.sleep` 到点后把消息(attempt+1)重投回主 topic `AGENTFLOW_SUBTASK`。这是最简单直接的"消费者侧睡眠"方案,但 `Thread.sleep` 期间该消费者线程完全占用、不再拉取新消息,若 `max.poll.interval.ms`(两次 `poll()` 之间的最大间隔)保持默认 5 分钟量级偏短的值,单条消息 sleep 时长一旦接近甚至超过这个阈值,broker 会认为该 consumer 已经"卡死"从而触发 rebalance、抢走它的分区——这与"正在合法睡眠等待重试"的语义冲突,所以要把 `max.poll.interval.ms` 调大(本项目设为 600000ms/10 分钟),确保最长档位(5 分钟)的 sleep 不会被误判为消费者假死。
   `pause()/seek()` 方案(把消息 seek 回原 offset、pause 该分区、在另一个调度线程里 timer 到点后 resume,而不阻塞 poll 循环)相比 sleep 的优势是不占用 poll 线程、能继续处理同分区之外的消息、且不需要靠调大 `max.poll.interval.ms` 这种"放宽误判阈值"的迂回手段;取舍在于实现复杂度高得多(需要自己管理定时器、分区暂停集合、避免 rebalance 时状态丢失),而且延迟精度依赖 `poll()` 被再次调用的时机(`resume` 之后仍要等下一次 `poll` 才会真正拉取)。本项目 W3–4 阶段选择"sleep + 独立延迟 topic"是刻意的复杂度取舍:三个档位量级都在秒到分钟级、单 worker 并发的独立延迟 consumer group 隔离了对主流程的阻塞影响,用一个配置项(调大 `max.poll.interval.ms`)换取实现简单,留了 backlog 供后续如果延迟精度/吞吐要求提高再切换到 pause/seek。

7. **`@Transactional` 修的是什么撕裂?超时扫描又兜什么?两者为何都要?**
   `@Transactional`(Task 1)修的是"同一次结果回传处理内部,DB 状态变更(子任务终态 + 计数自增 + 可能的任务终态聚合)与其副作用没有原子性保证"导致的撕裂:例如子任务状态已经 CAS 成 `COMPLETED`、计数也自增了,但聚合判定/写回任务终态那一步因为异常中途失败,就会出现"子任务已完成但任务永远停在 RUNNING"或"计数与实际子任务状态对不上"的不一致;把 `ResultHandleService.handle()` 整体纳入一个事务边界,保证这一组 DB 写入要么全部提交要么全部回滚,消费者侧再配合手动 ack(处理成功才提交 offset)避免"DB 已变更但 Kafka 消息被判定为未处理而重新投递"的二次撕裂。
   超时扫描(Task 4)兜的是完全不同的另一类问题——"消息层面彻底丢失或消费者彻底失联,连一次异常都不会有,自然也没有 `ResultHandleService` 可以 rollback 的机会":比如 worker 收到子任务消息后直接被 kill -9(没来得及产出结果也没来得及走 RetryRouter),或者 `SubtaskDispatcher` 发送到 broker 成功、但结果消息因为下游某种原因永远不会产生。这类场景里子任务永远停在 `DISPATCHED`,没有任何后续消息会触发 `ResultHandleService`,`@Transactional` 无从谈起,必须有一个独立于消息流的轮询兜底(`TimeoutSweepService` 定时扫 `updated_at` 超过 `stuck-seconds` 的 `DISPATCHED` 行)去主动发现并处理。
   两者缺一不可:`@Transactional` 保证"有事件发生时,处理这个事件的多步 DB 写入是原子的",解决的是并发/异常下的部分写入撕裂;超时扫描保证"即使永远没有事件发生,系统也不会永远卡住",解决的是"事件本身丢失"这一更根本的问题——前者是关于"一次处理内部的一致性",后者是关于"处理是否会发生"的兜底,层次不同,不能互相替代。

8. **DLQ 恢复为何要"重置子任务+回退计数+复活任务",而不是简单重投消息?**
   如果只是简单地把原始消息重新发一遍到主 topic 而不碰 DB 状态,会出现两类问题:(a) 子任务在 DB 里仍然是 `FAILED`、任务仍然是终态(`FAILED`/`PARTIAL_FAILED`)——`ResultHandleService.handle()` 的 CAS 要求源状态是 `DISPATCHED` 才能推进,一个已经是 `FAILED` 的子任务不会再被任何后续结果消息驱动状态变化,重投的消息处理完之后产生的结果消息只会被当作"过期重复结果"忽略掉,恢复完全不起作用;(b) 任务层面的计数(`subtask_failed`)和 `result` 快照里还留着旧的失败记录,即使子任务真的又跑了一遍,`done+failed==total` 的判定基准也是错的,没法重新走到"聚合出新终态"这一步。所以 DLQ 恢复必须先把子任务 FAILED→DISPATCHED(清空 `error_msg`、`redispatch_count` 归零,让它回到"可以被正常状态机推进"的位置——之所以是 `DISPATCHED` 而不是 `PENDING`,是因为 `replay()` 在同一次调用里紧接着就把消息重投出去了,消息已经"在途",必须精确对齐到 `ResultHandleService.handle()` 的 CAS 能识别的源状态)、任务失败计数原子回退 + 状态复活为 RUNNING(清空旧 `result`,让它重新具备"可以再次被判定终态"的资格),这一步做完之后重投消息才有意义——重投的消息能被正常处理、处理完的结果消息能被 `ResultHandleService` 正常 CAS 接受并推进计数,最终重新聚合出正确的任务终态。(本轮实盘演练也印证了这一点的重要性:见上方 backlog 关于 `DlqRecoveryService.replay()` 状态迁移缺口的记录——曾经把重投前的状态误设成了 `PENDING` 而非 `DISPATCHED`,导致后续结果被判定源状态不匹配而丢弃,这个 P1 已于 W3 修复,间接证明了"状态必须精确对齐到状态机能识别的位置"这件事本身的必要性。)

## W4 里程碑交付清单(Task 1–7,可观测与治理,已通过 `scripts/trace-demo.sh` + 手工演练验收)

- [x] MongoDB 基础设施 + `TraceEvent` 契约(Task 1):`AGENTFLOW_TRACE` topic、stage 枚举、common 模块 DTO
- [x] Trace 事件溯源(Task 2):`TraceEmitter`(server/worker 各一份)→ Kafka → server `TraceConsumer` → MongoDB(`trace_event`,一事件一文档)
- [x] server 侧全链路埋点(Task 3):submit/dispatch/settle/finalize/timeout/recovery 均 emit
- [x] worker 侧埋点(Task 4):WORKER_RECEIVED/PROCESSED/RESULT_SENT/RETRY/DLQ
- [x] Trace 回放 API(Task 5):`GET /api/v1/tasks/{taskUuid}/trace`,traceId=taskId、对外用 uuid 反查
- [x] 统一副作用 Policy 层(Task 6):`SideEffectPolicy.execute(businessKey, action)`(Redisson `setIfAbsent`+TTL 业务幂等,失败释放占位键),`NotificationService` 接入 `TaskFinalizer`
- [x] 端到端验收 + 里程碑收尾(Task 7,本轮):`scripts/trace-demo.sh` 实盘跑通,trace 完整覆盖 `SUBMITTED→…→TASK_FINALIZED`;通知去重经 `SideEffectPolicyTest`/`NotificationDedupeTest` 验证(见 `.superpowers/sdd/task-7-report.md`)

**Task 7 实盘发现(非缺陷,记录供后续关注)**:`GET /trace` 按 `timestamp`(epoch millis)排序,
而不是按 Mongo 插入序或因果序。实盘一次 3 子任务并发跑出的 trace 里出现过
`WORKER_RECEIVED(subtaskId=910, ts=X)` 排在其余两个子任务 `DISPATCHED(ts=X+2ms)` **之前**——
两次 emit 落在同一毫秒量级、跨进程(server 发 DISPATCHED、worker 发 WORKER_RECEIVED)时钟/网络
抖动导致时间戳非严格因果序。当前规模(毫秒级抖动、演示/审计用途)可接受;若后续要用 trace 做
精确因果重建,需要引入单调递增的序号(如 Kafka offset 或 traceId 内自增 seq)而非仅靠墙钟时间戳
排序。

## 自检(W4 里程碑,对应 `task-7-brief.md` 自检题)

9. **Trace 为何经 Kafka 事件溯源而非各处直写 Mongo?"保存时机"怎么答?**
   若各组件(server 的 submit/dispatch/finalize、worker 的 receive/process)各自直连 Mongo 写
   `trace_event`,会出现两个问题:(a) 写入点分散在 6+ 个类里,Mongo 连接/写入失败处理、schema
   演进、索引策略要在每处重复考虑,职责不单一;(b) worker 侧要额外引入 Mongo 依赖(网络、认证、
   连接池),而 worker 本来只需要已有的 `KafkaTemplate`。经 Kafka 事件溯源把"产生轨迹事件"和
   "落盘 Mongo"解耦成两段:各组件只管在状态变更那一刻 `emit(...)` 一条 `TraceEvent` 到
   `AGENTFLOW_TRACE`,不关心落盘细节;server 侧唯一的 `TraceConsumer` 是 Mongo 的唯一写入方。
   "保存时机"的硬答案是:**先落 Kafka,再落 Mongo**——`TraceEmitter.emit()` 在状态变更"即刻"把
   事件发到 Kafka(Kafka 落盘持久化),即使 `TraceConsumer` 进程或 Mongo 暂时不可用,事件也不会
   丢(Kafka 分区里保留、consumer 恢复后继续消费);而不是"等状态变更的主事务提交之后再异步补写
   Mongo"这种更容易丢失窗口的时机。emit 本身包一层 try-catch,失败只 warn 不拖垮主业务(trace
   属于旁路能力,不能反过来影响主链路可用性)。

10. **怎么从 trace 证明"闭环执行成功"?**
    `GET /api/v1/tasks/{taskUuid}/trace` 把该任务(`traceId=taskId`)下所有事件按时间排序返回;
    "闭环执行成功"体现在事件序列**完整覆盖**生命周期的两端和中间关键跳变:起点必须有
    `SUBMITTED`(任务被接收)、`DECOMPOSED`(拆解出子任务数),中间每个子任务都应能看到
    `DISPATCHED → WORKER_RECEIVED → PROCESSED → RESULT_SENT → SUBTASK_SETTLED` 这一串,终点必须
    落到 `TASK_FINALIZED`(带最终 `status`,如 `COMPLETED`/`PARTIAL_FAILED`/`FAILED`)。只要这条链
    路径完整、没有中途断掉(比如某子任务卡在 `DISPATCHED` 之后再没有任何后续事件),就能审计判断
    "这个任务确实从提交一路推进到了终态,而不是卡死或静默丢失"。本轮 Task 7 实盘跑的
    trace(`taskUuid=b0e7522c-...`)正是这个证据:能看到 `SUBMITTED→DECOMPOSED→` 3 个子任务各自
    `DISPATCHED→WORKER_RECEIVED→PROCESSED→RESULT_SENT→SUBTASK_SETTLED`,最后一条是
    `TASK_FINALIZED status=COMPLETED`,链路无断点。

11. **Policy 的业务幂等键与 W3 消费幂等键有何不同层次?**
    两者都是"Redis `setIfAbsent`/`exists` 判重 + TTL"的实现手法,但挡的是完全不同层次的重复:
    W3 的 `IdempotencyGuard`(worker 侧,key = `MD5(subtaskUuid+input)`)挡的是**消息层面**的重复
    ——同一条 Kafka 消息因为 rebalance、offset 未提交等原因被重新投递,导致同一次"业务动作"被
    执行两遍;它的粒度是"这条具体消息对应的这次处理"。Policy 的 `SideEffectPolicy`(server 侧,
    key 如 `notify:<taskUuid>`)挡的是**业务动作层面**的重复——不管这次"触发通知"是从哪条路径
    (正常结果回传 finalize、超时兜底 finalize、还是未来的 DLQ 恢复导致的二次 finalize)引发的,
    只要业务语义上是"同一个任务的完成通知",就只能真正执行一次。换句话说,消费幂等防的是"同一
    条消息不被多次消费",业务幂等防的是"同一个业务事实(任务已完成)不管被多少条不同路径/消息
    触发,副作用只发生一次"——前者是消息通道层面的去重,后者是业务语义层面的去重,可以同时
    发生(一条消息被重复消费,又恰好这条消息触发的动作也被业务幂等去重,双保险)。

12. **"所有副作用经同一 policy"如何代码层 enforce?(并说明 claim+失败释放键 的取舍)**
    Enforce 的方式不是"文档约定"而是**结构性收口**:`NotificationService`(以及未来任何新增的
    副作用,如真的建 case/发邮件)不直接执行动作,而是把"要做的事"包成一个 `Runnable` 传给
    `SideEffectPolicy.execute(businessKey, action)`,真正的 `log.info(...)`(或未来的真实外部调用)
    写在这个 lambda 里面、只能通过这一条路径被调用到。因为 `SideEffectPolicy` 是 Spring 单例、
    Redisson `RBucket` 的 key 前缀(`agentflow:sideeffect:`)和 `setIfAbsent` 判重逻辑全部封装在
    `execute()` 内部,业务代码拿不到、也不需要拿到 Redis 客户端本身,自然没有绕过去的旁路——
    "只能经这一个类"是靠"副作用动作的唯一入口就是这个方法签名"这件事本身保证的,而不是靠
    review 或命名约定。
    **claim(先占位再执行) vs 失败释放键的取舍**:`execute()` 先 `setIfAbsent` 占住 key(视为
    "认领"这次执行权),再运行 `action.run()`;若 `action` 抛异常,`bucket.delete()` 释放刚占的
    键,再把异常继续往上抛。选择"先 claim 后执行、失败释放"而不是"先执行成功后才置键"
    (类似 W3 `IdempotencyGuard.markProcessed` 的"成功后置位"),是因为两者要解决的并发场景不同:
    这里 `execute()` 本身不是幂等重试的唯一入口保护——真正会重复调用 `execute()` 的是"业务层面
    的多次触发"(如同一任务被多个线程同时判定终态、或未来 DLQ 恢复二次 finalize),这些触发
    可能是并发的,如果不先占位就可能出现"两个线程都检查到 key 不存在,都去执行"的竞态;先
    `setIfAbsent` 把"认领执行权"这件事原子化,保证并发下只有一个线程能进入 `action.run()`。
    代价是如果 `action` 执行到一半进程崩溃(没有走到 catch 分支的 `delete()`),占位键会一直
    留到 TTL(24h)才过期,期间这个业务动作会被误判"已完成"而不会重试——这是"防并发重复执行"
    与"允许失败后立即重试"之间的取舍,当前用"失败时显式释放"覆盖了"同步异常"这一大类场景,
    未覆盖的是"进程整体崩溃、来不及执行 catch"这种更少见的场景,留给 TTL 兜底(24h 后允许
    重新触发),接受这个有界的"迟到重试"作为代价。

## W3 最终评审遗留(defer)

- **C3**:`DlqRecoveryService.replay` 无 CAS,并发双触发会让 `subtask_failed` 变负 → 建议加
  `casStatus(FAILED→DISPATCHED)==1` 守卫(当前为运营手动触发、低并发场景,暂可接受,列入待办)。
- **C4**:超时扫描是单个大事务遍历所有卡死子任务 + 事务内 Kafka 发送,规模大时会形成长事务
  (里程碑当前规模可接受,后续量级上升需拆分批次/事务外发送)。
- **T4b**:超时重投的 `SubtaskMessage` 硬编码 `type="ECHO_BATCH"`(DLQ `replay` 已正确使用
  `t.getType()`);当前只有一种任务类型尚不触发问题,但加入第二种任务类型时是潜在 bug,
  留到 W5+ 修。
- **T3**:`RetryRouter` 的 `switch default` 分支吸收了 `attempt≥2` 的情况,当前依赖前置守卫保证
  安全,尚未出问题但语义不够显式。
- **T5a**:目前没有 `DlqControllerTest`,DLQ 恢复的 HTTP 层缺少直接测试覆盖。
