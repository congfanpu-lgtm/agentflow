# W4 可观测与治理 · 设计文档(子计划二)

> 创建:2026-07-12 · 状态:定稿(基于 master 设计文档已定规格)
> 上游:`docs/specs/2026-07-11-agentflow-design.md`(Run Trace 卖点4 + 统一副作用 Policy 层)
> 前置:W1–2 骨架 + W3 可靠性核心已在 `main`(commit ba81afe),全库 43 测试绿
> 执行:直接在 `main` 分支

---

## 0. 定位

W3–4「可靠性」里程碑的第二个子计划:**可观测 + 治理**。
覆盖 master 设计文档的 Run Trace(卖点4)+ 统一副作用 Policy 层。
面试叙事:可控 / 可观测 / 可审计(腾讯面经核心:"执行轨迹怎么保存、保存时机""闭环是否执行成功""所有副作用路径经过同一套 policy""重复入队会不会重复发邮件")。

**设计选择说明(自主决策,供 review)**:当前系统是 echo pipeline,尚无真实外部副作用。本子计划用**代表性副作用**(任务完成时的"通知/建 case")演示 Policy 层的统一出口 + 业务幂等;Run Trace 记录 echo pipeline 的真实生命周期步骤。二者到 W5–6 真 Agent 落地时可直接复用、加真副作用。

---

## 1. 范围

| # | 特性 | 一句话 |
|---|------|--------|
| A | Run Trace(事件溯源 → MongoDB) | 每个生命周期步骤留痕,可回放,证明闭环执行成功 |
| B | 统一副作用 Policy 层 | 所有副作用经同一闸 + 业务级幂等(防重复通知/建 case) |

**不在范围**:分片 MongoDB(单机 + 分片谈资);真 LLM/Agent(W5–6);ZK(W9)。

---

## 2. Run Trace 设计(卖点4)

### 架构:事件溯源,统一经 Kafka TRACE topic → 单一 Mongo 消费者

```
各组件(server+worker)每个状态变更 → emit TraceEvent → Kafka AGENTFLOW_TRACE
                                                            ↓
                                            server TraceConsumer → MongoDB(单机)
                                                            ↓
                                        GET /api/v1/tasks/{uuid}/trace  ← 回放/审计
```

**为什么事件溯源经 Kafka**(而非各处直写 Mongo):
- **统一出口**:所有留痕走一条流,Mongo 只在一处被写(TraceConsumer),职责单一。
- **保存时机 = 状态变更即刻 emit**;Kafka 持久化保证即使消费者/进程崩溃,轨迹事件不丢(先落 Kafka,再落 Mongo)。这是"保存时机"面试点的硬答案。
- worker 无需 Mongo 依赖,只需已有的 KafkaTemplate。

### TraceEvent(common 模块 DTO)
`traceId`(=taskUuid)· `taskId` · `subtaskId`(可空)· `stage`(枚举)· `status` · `detail`(简短)· `timestamp`(epoch millis)。

**stage 枚举**(覆盖真实生命周期):
`SUBMITTED / DECOMPOSED / DISPATCHED / WORKER_RECEIVED / PROCESSED / RESULT_SENT / SUBTASK_SETTLED / RETRY / DLQ / RECOVERED / TIMEOUT_REDISPATCH / TASK_FINALIZED`

### MongoDB 存储
- 单机 `mongo:7`,加入 docker-compose(端口 27017)。
- collection `trace_event`,**一事件一文档**(写多、无更新竞争、天然按 traceId 聚合)。索引 `traceId + timestamp`。
- 查询:`find(traceId).sort(timestamp)` = 完整回放。
- 分片留作设计谈资(量大后按 traceId 分片),不实作。

### 埋点(emit 点)
| 组件 | stage |
|---|---|
| TaskSubmitService | SUBMITTED, DECOMPOSED |
| SubtaskDispatcher | DISPATCHED |
| SubtaskListener(worker) | WORKER_RECEIVED, PROCESSED, RESULT_SENT |
| RetryRouter(worker) | RETRY(每档), DLQ |
| ResultHandleService | SUBTASK_SETTLED, TASK_FINALIZED |
| TimeoutSweepService | TIMEOUT_REDISPATCH |
| DlqRecoveryService | RECOVERED |

### 回放 API
`GET /api/v1/tasks/{taskUuid}/trace` → 按时间排序的事件数组。**审计价值**:能从 trace 证明"闭环是否执行成功"——每步是否都留痕、是否走到 TASK_FINALIZED。

---

## 3. 统一副作用 Policy 层

### 核心:所有副作用经同一闸 + 业务级幂等

```
业务代码 → SideEffectPolicy.execute(businessKey, action)
                 ↓  查 Redis:businessKey 见过?
           见过 → 跳过(幂等,不重复执行副作用)
           没见 → 执行 action + 记 businessKey(TTL)
```

- **副作用**定义:有外部/持久后果的动作(发通知、建 case、写外部系统)。**业务幂等键**由业务语义决定(如 `notify:<taskUuid>`),与 W3 的"消息幂等键"(防重复消费)是不同层次——这个防"同一业务动作被触发多次"。
- **代码层 enforce**:副作用只能经 `SideEffectPolicy.execute` 执行,不给旁路(面经"MCP 路径和队列路径都经同一套 policy")。
- **Redisson** 复用(server 加 Redisson;或用 spring-data-redis 的 `SETNX`)。用 `setIfAbsent(key, ttl)` 原子占位。

### 代表性副作用:任务完成通知
- 新增 `NotificationService.notifyTaskFinished(task)`——模拟"发报告通知 / 建 case"(本阶段= 记一条通知日志 + 落一条 `notification` 记录/Mongo 或日志)。
- **必须经 Policy 闸**:`policy.execute("notify:" + taskUuid, () -> reallyNotify(task))`。
- 接入点:`TaskFinalizer` 判定终态后调用。
- **演示幂等**:DLQ 恢复导致同一任务二次 finalize 时,`notify:<uuid>` 已存在 → **不重复发通知**(直接命中面经"同一 report 重复入队会不会重复发邮件")。

---

## 4. 任务顺序

```
① docker-compose 加 MongoDB + server 加 spring-data-mongodb;TraceEvent DTO + AGENTFLOW_TRACE topic
② TraceEmitter(emit 到 Kafka)+ server TraceConsumer → Mongo(TraceDocument + Repository)
③ 全链路埋点(server 各处 + worker 各处 emit)
④ Trace 回放 API(GET /trace)+ 测试
⑤ SideEffectPolicy 闸(Redis 业务幂等)+ NotificationService,接入 TaskFinalizer
⑥ 端到端:提交→查 trace 见完整生命周期;二次 finalize 通知不重复。里程碑验收 + 文档
```

## 5. 测试策略
- 单元:TraceEmitter 事件构造;SideEffectPolicy 幂等(见过跳过 / 没见执行)。
- 集成(真 Mongo + Kafka + Redis):emit → TraceConsumer 落 Mongo → 查询回放完整;Policy 重复 businessKey 只执行一次。
- E2E:一个任务的 trace 覆盖 SUBMITTED→...→TASK_FINALIZED;模拟二次 finalize → 通知只一条。

## 6. 与上游对齐
- 落实 master 卖点4(Run Trace)+ 统一副作用 Policy 层。
- Harness/Runtime 边界(master 图):Trace/Policy 属 Harness(管控面),worker Agent 执行属 Runtime——留痕与副作用统一回 Harness 经 Policy,呼应 master"Runtime 只想和做,副作用回 Harness"。

## 7. 风险与纪律
| 风险 | 兜底 |
|---|---|
| 范围膨胀(trace 维度易膨胀) | 只记 stage 枚举内的事件;不做 UI/检索,只做回放 API |
| Mongo 引入成本 | 单机;只加一个容器;spring-data-mongodb 标准用法 |
| Policy 抽象过度 | 只做一个代表性副作用(通知),够演示统一出口+业务幂等即可 |
| 直接在 main | 每任务 TDD + 审查 + 频繁提交 |
