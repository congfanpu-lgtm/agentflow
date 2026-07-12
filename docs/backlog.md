# Backlog(范围纪律:W10 前只记不做)

- DAG 依赖边调度(当前仅 fan-out/fan-in);SubtaskDef 预留 dependsOnSeq
- Kafka UI 容器(如 provectuslabs/kafka-ui,可视化 topic/consumer group,arm64 兼容)
- Worker RUNNING 状态上报(心跳);W9 可选 Zookeeper 注册/选主
- 自研 Kafka 延迟重试 topic + DLQ(W3–4,spec 卖点5)
- 为 maven-compiler-plugin 开启 <parameters>true</parameters>(否则新增 @PathVariable/@RequestParam 不写显式名会在 Spring 6.1 运行时失败)

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
