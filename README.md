# AgentFlow

分布式多 Agent 任务编排平台(开发中,W1–2 骨架已通)。
设计文档见 `docs/specs/2026-07-11-agentflow-design.md`。

## 架构(当前骨架)

用户 → API(8080) → 协调器拆解 → Kafka(AGENTFLOW_SUBTASK) → Worker 池
→ 回传(AGENTFLOW_RESULT) → 状态机聚合 → 查询结果

## 快速开始

前置:JDK 17+、Maven 3.9+、Docker(OrbStack / Docker Desktop)。

    docker compose up -d                          # MySQL/Redis/Kafka(KRaft)/MongoDB
    mvn -q -pl agentflow-server -am spring-boot:run   # 终端 A
    mvn -q -pl agentflow-worker -am spring-boot:run   # 终端 B
    ./scripts/e2e-smoke.sh                        # 终端 C:端到端验证

提交任务:

    curl -X POST localhost:8080/api/v1/tasks -H 'Content-Type: application/json' \
      -d '{"type":"ECHO_BATCH","payload":{"items":["hello","world"]}}'

查询:`GET /api/v1/tasks/{taskUuid}`

## 多 Worker 水平扩展

主 topic 3 分区,最多 3 个 Worker 并行(同消费组自动 rebalance):

    # 终端各起一个,同 group 自动分走分区
    mvn -q -pl agentflow-worker -am spring-boot:run
    mvn -q -pl agentflow-worker -am spring-boot:run
    mvn -q -pl agentflow-worker -am spring-boot:run

优雅停机:`Ctrl-C` 后 Worker 处理完在途子任务、提交 offset 再退出,配合幂等去重保证不丢不重。
(1→3 Worker 吞吐提升比在 W8 压测量化。)

## 可观测与治理(Run Trace + Policy)

**Run Trace(事件溯源 → MongoDB 回放)**:server/worker 各组件在每个生命周期节点(提交、拆解、
分发、worker 接收/处理/回传、子任务落定、超时重投、DLQ 恢复、任务终态落定……)统一经
`TraceEmitter.emit(traceId, taskId, subtaskId, stage, status, detail)` 发一条 `TraceEvent` 到
Kafka `AGENTFLOW_TRACE` topic;server 侧唯一的 `TraceConsumer` 消费落 MongoDB(`trace_event`
集合,一事件一文档)。`traceId = String.valueOf(taskId)`,对外用 `taskUuid` 反查。
`GET /api/v1/tasks/{taskUuid}/trace` 按时间排序返回完整轨迹,可回放/审计"闭环是否执行成功"
(是否一路走到 `TASK_FINALIZED`)。经 Kafka 而非各处直写 Mongo 的原因:统一出口(Mongo 只在
`TraceConsumer` 一处被写)、保存时机是"状态变更即刻 emit、先落 Kafka 落盘再落 Mongo"(消费者/
进程崩溃也不丢事件)、worker 侧无需引入 Mongo 依赖。

**统一副作用 Policy 层**:所有有外部/持久后果的动作(通知、建 case 等)必须经
`SideEffectPolicy.execute(businessKey, action)` 这一个闸执行——不给旁路。用 Redis
`setIfAbsent(key, ttl)` 做业务级幂等:同一 `businessKey`(如 `notify:<taskUuid>`)首次执行
并占位,重复触发直接跳过("副作用去重跳过"日志),防止重复入队/二次 finalize 导致重复发通知。
执行失败会释放占位键,保证失败的副作用允许被重试而不会被"假幂等"卡死。代表性接入:
`TaskFinalizer` 判定终态后调用 `NotificationService.notifyTaskFinished`,均经 Policy 闸。

演示脚本:`./scripts/trace-demo.sh`(提交任务 → 轮询到 COMPLETED → 打印完整 trace)。

## 测试

    mvn test        # 需 docker compose up -d(集成测试直连本地 MySQL/Kafka/Mongo)

## 路线图

- [x] W1–2 骨架:API + 状态机 + Kafka 分发 + echo Worker
- [x] W3–4 可靠性核心: 幂等、Kafka 自研重试/死信+恢复、超时兜底、优雅停机
- [x] W3–4 可观测:Run Trace(事件溯源→Mongo 回放)+ 统一副作用 Policy 层
- [ ] W5–6 LLM 网关(令牌桶/路由/token 记账)+ LangChain4j Agent
- [ ] W7 RAG 检索服务(FastAPI + pgvector)
- [ ] W8 压测与容错演练
