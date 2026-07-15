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

**Web 控制台**:server 起来后浏览器打开 `http://localhost:8080/`——单文件看板(原生 JS,零依赖,
server `static/` 直供):提交任务(ECHO/LLM/RESEARCH)、实时进度条、Run Trace 执行轨迹时间线、
LLM token 记账,轮询刷新。用于 demo 演示。

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

## LLM 网关与可控 Agent(W5–6)

**LLM 网关(唯一出口)**:所有 LLM 调用只能经 `LlmGateway.chat(ChatRequest)`——业务代码拿不到底层
`LlmClient`,没有旁路(代码层 enforce)。网关职责:① 多模型路由(按 `model` 选桶 + 计价)
② 限流 ③ 调用 ④ token 记账。默认后端 `MockLlmClient`(确定性、不烧钱、无需 API key,压测/单测都用它);
`llm.provider=openai` 切真实模型(OpenAI 兼容协议,JDK HttpClient,base-url/key 走环境变量)。

**Redis + Lua 令牌桶限流**:桶状态存 Redis(跨 worker 实例共享,水平扩展下限流才是全局的),
"读令牌→按时间补充→判够→扣减→回写"整段用一个 Lua 脚本原子执行(Redis 单线程 = 天然原子,
免分布式锁,与状态机用 MySQL CAS 同源)。扣不到令牌 → `RateLimitedException`,worker 侧当普通失败
交 W3 阶梯重试(复用可靠性层,不另造机制)。

**token 记账**:每次调用把 prompt/completion token 数 + 估算成本(微美元)原子累加到 Redis;
`GET /api/v1/llm/usage` 按模型返回累计 token 与成本(简历指标3/6 数据源)。

**AgentContext(防上下文膨胀)**:`AgentContext.build(system, input, history)` 按 token 预算裁剪——
超预算优先丢最老历史,历史丢光仍超则截断超长输入(保留头尾),裁剪量记日志。绝不无上限塞给模型。

**Skill 动态注册 + schema 约束输出**:实现 `Skill` 接口 + `@Component` 即被 `SkillRegistry` 自动收录
(动态加载);多个 skill 命中同一 intent 按 `priority` 排序;每个 skill 声明 `outputSchema`,LLM 输出
必须过 `SchemaValidator` 校验,不合规视为失败交重试——**输出稳定是代码层校验,不是求模型听话**。

**真 Agent**:新任务类型 `LLM_BATCH`,worker `LlmProcessor`(Runtime)= AgentContext 组 prompt →
网关调模型 → 按 skill schema 校验输出;`SubtaskListener` 按 `msg.type` 路由到处理器(策略模式,
Echo/LLM 并存)。副作用一律回 Harness 经 Policy——处理器不产生外部副作用。

提交 LLM 任务(默认 mock):

    curl -X POST localhost:8080/api/v1/tasks -H 'Content-Type: application/json' \
      -d '{"type":"LLM_BATCH","payload":{"items":["文本A","文本B"],"model":"mock-small"}}'

演示脚本:`./scripts/w5-6-demo.sh`(提交 LLM_BATCH → 打印结果 + trace + `GET /llm/usage` 记账)。

## RAG 检索与多角色 Agent(W7)

**独立 Python/FastAPI RAG 微服务(`agentflow-rag/`)**:跨语言架构——Java 主链路 + Python
embedding 生态。`/rag/ingest` 灌库、`/rag/search` 近邻检索、`/health` 探活;向量存 **pgvector**
(`documents(id,content,embedding vector(256))`,余弦 `<=>` ANN)。embedding 默认**确定性词哈希投影**
(离线、可复现、无需 API key,延续 mock-first 哲学),真实 embedding 是 backlog 配置开关。
自建小标注集测 **Recall@K**(`tests/test_recall.py`,确定性 embedding 下 mean Recall@3=1.0)。

**意图驱动检索 + 多角色**:worker 经 `RagClient`(唯一检索入口,与 LLM 经网关同构)调 RAG 服务。
新任务类型 `RESEARCH_BATCH` 由 `ResearchProcessor` 处理,是一条子任务内的角色流水:
router(意图判断是否需要检索)→ retriever(**命中才检索**,不是每步都查)→ AgentContext 注入资料
→ summarizer(经网关调 LLM)→ RagSearchSkill schema 校验。跨子任务的聚合(汇总)由 `TaskFinalizer`
完成。检索只读、不产生副作用故不经 Policy。

启动 RAG 服务(前置:pgvector 容器):

    # pgvector 容器(compose 文件本机被占用,用 docker run;等价 compose 见下)
    docker run -d --name agentflow-pgvector -p 5433:5432 \
      -e POSTGRES_PASSWORD=agentflow -e POSTGRES_DB=agentflow_rag pgvector/pgvector:pg16
    cd agentflow-rag && python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
    ./run.sh                                       # RAG 服务(:8000)

> 等价 docker-compose 片段(供后续合入 `docker-compose.yml`):
> ```yaml
>   pgvector:
>     image: pgvector/pgvector:pg16
>     container_name: agentflow-pgvector
>     ports: ["5433:5432"]
>     environment:
>       POSTGRES_PASSWORD: agentflow
>       POSTGRES_DB: agentflow_rag
> ```

提交研究任务(mock LLM + 确定性 embedding,离线可跑):

    curl -X POST localhost:8080/api/v1/tasks -H 'Content-Type: application/json' \
      -d '{"type":"RESEARCH_BATCH","payload":{"questions":["kafka 如何重分配分区?","hello there"],"model":"mock-small"}}'

演示脚本:`./scripts/w7-rag-demo.sh`(灌库 → RESEARCH_BATCH:需检索项 `usedRag=true` 带
`retrieved` ids;不需项 `usedRag=false` 不检索)。

## 压测与指标(W8)

零依赖 Python 压测脚本 `scripts/load_test.py`(标准库并发提交+轮询,测**端到端完成延迟**而非
纯 HTTP 延迟),全程 mock LLM 不烧钱。实测(环境:Apple M5 / JDK 21 / 中间件全 Docker;负载 ECHO_BATCH,
每子任务模拟 300ms):

| 指标 | 实测 |
|---|---|
| 吞吐/延迟(3 worker) | 提交 P50 40ms/P99 137ms;端到端 P50 5.3s/P99 6.9s;103 任务/分;8.6 子任务/秒 |
| 水平扩展 1→3 worker | 子任务吞吐 3.1→8.6/秒 = **2.77×**(单机资源瓶颈,非架构瓶颈) |
| 成本控制(幂等拦截) | 崩溃场景 45/165 = **27.3%** 重复投递被幂等拦住,省冗余处理(≈LLM 调用) |
| 容错(零丢失) | kill -9 一个 worker 于压测中途 → **24/24 任务仍 COMPLETED** |
| 检索质量 | mean **Recall@3 = 1.0**(W7,确定性 embedding) |
| 副作用幂等 | 85 任务落定 → **85 通知,0 重复** |
| 可观测 | trace 完整率 **100%**(每任务 SUBMITTED→…→TASK_FINALIZED) |

> 数字与机器强相关,绝对值不跨机可比;架构结论看**相对值**(扩展比、拦截率、零丢失、trace 100%)。
> 复现:`python3 scripts/load_test.py --tasks 30 --items 5 --concurrency 10 --trace-check`(先起 server + N worker)。

## 测试

    mvn test        # Java:需 MySQL/Kafka/Mongo/Redis + pgvector 容器(集成测试直连本地中间件)
    cd agentflow-rag && .venv/bin/python -m pytest -q   # Python RAG:需 pgvector 容器

> 注:本机 Maven 运行时若为 JDK 21+ 之外版本(lombok 兼容),构建用 `JAVA_HOME=$(/usr/libexec/java_home -v 21)`。

## 路线图

- [x] W1–2 骨架:API + 状态机 + Kafka 分发 + echo Worker
- [x] W3–4 可靠性核心: 幂等、Kafka 自研重试/死信+恢复、超时兜底、优雅停机
- [x] W3–4 可观测:Run Trace(事件溯源→Mongo 回放)+ 统一副作用 Policy 层
- [x] W5–6 LLM 网关(Redis+Lua 令牌桶/多模型路由/token 记账)+ 真 Agent(经网关,mock 默认)+ AgentContext + Skill 注册/schema 约束
- [x] W7 RAG 检索服务(Python/FastAPI + pgvector,确定性 embedding + Recall@K)+ 意图驱动检索 + 多角色处理器
- [x] W8 压测与容错演练:7 项指标实测(见"压测与指标",水平扩展 2.77×、零丢失、trace 100%)
