# AgentFlow 简历 Bullets(中英,W10 定稿)

> 原则:数字均来自实测(`scripts/load_test.py`,Apple M5 / JDK 21 / mock LLM),可复现、面试可讲。
> 绝对值机器相关,叙事用相对值(扩展比 / 拦截率 / 零丢失 / trace 100%)。按投递市场二选一或合并。

---

## 🇺🇸 English (US market — systems design / AI platform / SDE)

**AgentFlow — Distributed Multi-Agent Task Orchestration Platform** · *Java 17, Spring Boot, Kafka, Redis, MySQL, MongoDB, pgvector, Python/FastAPI*

- Built a distributed platform that decomposes tasks into sub-task DAGs and dispatches them over
  **Kafka (KRaft)** to a horizontally-scalable worker pool; measured **2.77× throughput** scaling
  from 1→3 workers via consumer-group rebalancing.
- Designed a **self-built Kafka reliability layer** — staged delay-retry topics (5s/30s/5m) + dead-letter
  queue + replay — compensating for Kafka's lack of native delay/DLQ; a `kill -9` fault drill mid-load
  finished **24/24 tasks with zero loss**.
- Implemented a **cost-aware LLM gateway** as the single choke-point for all model calls: **Redis+Lua
  token-bucket** rate limiting (atomic, shared across workers), multi-model routing, and token accounting;
  content-hash idempotency intercepted **27.3%** of redeliveries under crash, eliminating duplicate LLM calls.
- Engineered **event-sourced execution tracing** (Kafka → MongoDB) with **100% trace completeness**,
  and a **unified side-effect policy** with business-level idempotency fired after commit — **85 tasks →
  85 notifications, 0 duplicates**.
- Added an **intent-driven RAG tool** (Python/FastAPI + pgvector, retrieve only when intent warrants),
  a token-budget **context-trimming** layer, and a **schema-constrained dynamic skill registry** so model
  output is validated in code rather than trusted (retrieval quality: see the 2026-10 section below).

## 🇨🇳 中文(国内市场 — 高并发 / 后端 / Agent 工程)

**AgentFlow — 分布式多 Agent 任务编排平台** · *Java 17、Spring Boot、Kafka、Redis、MySQL、MongoDB、pgvector、Python/FastAPI*

- 自研分布式任务调度:协调器将任务拆解为子任务 DAG,经 **Kafka(KRaft)** 削峰分发至可水平扩展的
  Worker 池,消费组 rebalance 承载扩缩容,实测 **1→3 Worker 吞吐提升 2.77×**。
- 在 Kafka 之上**自研阶梯延迟重试(5s/30s/5m)+ 死信队列 + 恢复重放**,补齐 Kafka 无原生延迟/死信;
  压测中途 `kill -9` 一个 Worker,任务 **24/24 零丢失**(消息重投 + 幂等兜底)。
- 自研 **LLM 成本管控网关**作为所有模型调用的唯一出口:**Redis+Lua 令牌桶**限流(原子、跨实例共享)、
  多模型路由、token 记账;崩溃场景下内容哈希幂等**拦截 27.3% 的重复投递**,避免重复 LLM 调用。
- 实现 **Run Trace 执行轨迹**(事件溯源 Kafka→MongoDB,**完整率 100%**,可回放审计闭环)与**统一副作用
  Policy**(业务幂等 + afterCommit),实测 **85 任务 → 85 通知、0 重复**。
- 增加**意图驱动 RAG 工具**(Python/FastAPI + pgvector,命中意图才检索)、**上下文裁剪**(按 token 预算防膨胀)、
  **schema 约束的动态 Skill 注册**(输出代码层校验而非信任模型);检索效果见下方 2026-10 一节。

---

## 面试讲点速查(每个卖点 → 一句话硬答案)

| 卖点 | 一句话 |
|---|---|
| 状态迁移为何用 DB CAS | 把"读+判+写"压成一条原子 SQL,行锁天然互斥,抢输方 `rowsAffected==0` 直接感知,无需分布式锁 |
| 部分失败语义 | `done+failed==total` 且两者都 >0 → `PARTIAL_FAILED`,保留成功输出 + 暴露失败详情,支持只重放失败部分 |
| Kafka 无延迟/死信怎么办 | 独立延迟 topic + header 带 attempt/not-before,消费者 sleep 到点重投;耗尽进 DLQ;`max.poll.interval.ms` 调大避免误判假死 |
| 令牌桶为何 Redis+Lua | 令牌桶是 check-then-act,单线程执行整段 Lua = 天然原子,桶在 Redis 跨 Worker 共享,免分布式锁 |
| 调用/副作用如何 enforce 经同一闸 | 业务代码只拿到 Gateway/Policy bean、拿不到底层 client,唯一方法签名即唯一入口,结构性收口非约定 |
| 保存时机(Trace) | 状态变更即刻 emit,先落 Kafka 再落 Mongo;emit 包 try-catch,trace 丢失不拖垮主链路 |
| 上下文防膨胀 | 按 token 预算裁剪:优先丢最老历史,历史丢光再截断超长输入(留头尾),裁剪量可观测 |
| Skill 输出稳定 | 声明 outputSchema,产出过代码层 SchemaValidator,不合规=失败交重试;不是"求"模型听话 |
| 意图驱动检索 | 检索是有成本的旁路工具,由意图门控,命中才查;不是每步都查 |
| 为什么混合检索 | BM25 擅长精确词(报错码、专有名词),向量擅长同义改写;RRF 只按名次融合,不需要校准两边分数 |
| 检索效果怎么证明 | BEIR 公开数据集算 nDCG@10;先复现论文 / 模型卡的基线(差距 <0.02)证明评测本身没写错,再给配对 bootstrap 置信区间 |
| 评测结果可信吗 | 裁判回到原网页核对引用;每轮约 70 条引用,严格准确率有 ±11 个点的误差,所以只把结构性变化(无来源 25%→12%)当结论 |

## 诚实边界(面试主动说,加分)
- 压测用 mock LLM——测的是调度/并发/容错,不是模型;扩展比 2.77×(非满 3×)是单机资源瓶颈非架构瓶颈。
- 检索模型 bge-small 只支持英文,纯中文查询效果差;SciFact 上混合检索比纯向量的提升不显著(置信区间跨 0);演示环境的知识库默认是空的。
- 研究 agent 的引用严格准确率只有 60%(宽松 87%),还有 12% 的 agent 没拿到来源,是已知的主要短板。
- 多角色是"子任务内角色流水",跨 Agent DAG 依赖边调度尚未做(backlog)。

---

## RESEARCH_AGENT(2026-10)

> 只写代码里真实存在、并且实测过的内容。数字来源:`scripts/chaos-agent.sh` / `scripts/e2e-agent.sh` 输出、
> `mvn -B test`、`eval/run_eval.py`(真实模型,README「Research-agent evaluation」v4)、
> `agentflow-rag/retrieval_eval.py`(BEIR,README「Retrieval evaluation」)。

**English**
- Built a **resumable multi-agent research pipeline** (plan → parallel ReAct agents → cited report) on the Kafka
  orchestration layer, with **native LLM tool calling** (Wikipedia, web fetch, pgvector RAG) and **per-step checkpoints
  in Redis**; a chaos test that `docker kill`s a worker mid-run shows another worker **resuming from the next step
  without re-running tools**.
- Made multi-stage orchestration **exactly-once at the stage boundary**: the next stage is created in the same DB
  transaction that settles the last result (row lock on the task counter), dispatched after commit; partial agent
  failures still yield a report (`PARTIAL_FAILED`).
- Hardened the agent against untrusted model/web output: **SSRF guard** re-checked on every redirect, tool errors fed back
  to the model, duplicate-call cache, and **citation verification** that drops URLs no tool actually returned.
- Built an **LLM-as-judge eval** (20 EN/ZH questions; the judge fetches each sampled cited page to check support) and
  let it drive design: a reflection stage cost **+85%** for no depth gain, so it ships off; truncated answers were silently
  dropping sources, and fixing that cut agents with no sources from **25% to 12%**. Final config: citation accuracy
  60% strict / 87% lenient (±11 pt), **$0.049** and 24 s p50 per question.
- Replaced hashed embeddings with **hybrid retrieval** (in-memory BM25 + local bge-small ONNX embeddings, fused by RRF):
  BEIR nDCG@10 **0.21 → 0.73** on SciFact and **0.07 → 0.37** on NFCorpus; BM25 and dense each reproduce published
  baselines within 0.02, and every gain carries a paired-bootstrap 95% CI.

**中文**
- 在 Kafka 编排层上实现**可续跑的多 agent 研究流水线**(拆题 → 并行 ReAct agent → 带引用报告),
  **原生 tool calling**(Wikipedia / 网页抓取 / pgvector 知识库),**每步存档到 Redis**;chaos 脚本中途
  `docker kill` worker,另一个 worker **从下一步续跑、已调用工具不重跑**。
- 多阶段编排在阶段边界**只推进一次**:下一阶段与"落定最后一个结果"同事务创建(任务计数行锁),提交后再分发;
  部分 agent 失败仍产出报告(`PARTIAL_FAILED`)。
- 对模型/网页输出做防护:**SSRF 防护**(每次重定向重新校验)、工具错误回传模型、重复调用缓存、
  **引用校验**(删除工具从未返回过的 URL)。
- 搭建 **LLM-as-judge 测评**(20 道中英文题,裁判抽查引用并回到原网页核对)并据此取舍:反思阶段成本 **+85%** 而深度
  无提升,默认关闭;回答被截断导致来源悄悄丢失,修复后无来源 agent 由 **25% 降至 12%**。最终配置:引用准确率
  严格 60% / 宽松 87%(±11 个点),每题 **$0.049**、p50 24 秒。
- 检索由哈希向量改为**混合检索**(内存 BM25 + 本地 bge-small ONNX 向量,RRF 融合):BEIR nDCG@10 SciFact
  **0.21 → 0.73**、NFCorpus **0.07 → 0.37**;BM25 与纯向量两路分别复现公开基线(差距 0.02 以内),每项提升都给出
  配对 bootstrap 95% 置信区间。

