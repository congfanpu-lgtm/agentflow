# AgentFlow

分布式多 Agent 任务编排平台(开发中,W1–2 骨架已通)。
设计文档见 `docs/specs/2026-07-11-agentflow-design.md`。

## 架构(当前骨架)

用户 → API(8080) → 协调器拆解 → Kafka(AGENTFLOW_SUBTASK) → Worker 池
→ 回传(AGENTFLOW_RESULT) → 状态机聚合 → 查询结果

## 快速开始

前置:JDK 17+、Maven 3.9+、Docker(OrbStack / Docker Desktop)。

    docker compose up -d                          # MySQL/Redis/Kafka(KRaft)
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

## 测试

    mvn test        # 需 docker compose up -d(集成测试直连本地 MySQL/Kafka)

## 路线图

- [x] W1–2 骨架:API + 状态机 + Kafka 分发 + echo Worker
- [x] W3–4 可靠性核心: 幂等、Kafka 自研重试/死信+恢复、超时兜底、优雅停机
- [ ] W5–6 LLM 网关(令牌桶/路由/token 记账)+ LangChain4j Agent
- [ ] W7 RAG 检索服务(FastAPI + pgvector)
- [ ] W8 压测与容错演练
