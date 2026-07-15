# W7 RAG 检索 + 意图驱动 + 多角色 Implementation Plan(子计划四)

> **For agentic workers:** 每任务 TDD → commit → push。Steps 用 checkbox(`- [ ]`)。

**Goal:** 加一个独立 Python/FastAPI RAG 微服务(确定性 embedding + pgvector 近邻检索),Java worker
经 `RagClient` 按意图门控调用它,新 `RESEARCH_BATCH` 任务经 `ResearchProcessor`(意图判断→检索→
注入 AgentContext→LLM 汇总)跑通,产出 Recall@K 指标。

**Architecture:** 见 `docs/superpowers/specs/2026-07-15-w7-rag-multiagent-design.md`。

**Tech Stack:** Python 3.12 · FastAPI · uvicorn · psycopg[binary] · numpy · pytest(RAG 服务);
Java 17 · JDK HttpClient(RagClient,无新 Maven 依赖)· pgvector/pgvector:pg16(端口 5433)。

## Global Constraints
- **直接在 `main` 分支**;当前 `main` @ `338872c`,全库 78 测试绿。
- **确定性 embedding 默认**(离线、可复现、无 key);真实 embedding 列 backlog 开关。
- **pgvector 经 `docker run`**(`agentflow-pgvector`,宿主端口 **5433**→容器 5432;compose 文件被占用,只在 README/backlog 记配置不改)。
- RAG 服务连库:`postgresql://postgres:agentflow@localhost:5433/agentflow_rag`。
- Java 侧 RagClient 是**唯一检索入口**;检索只读、不经 Policy。
- 处理器路由复用 W5-6 `SubtaskProcessor`;`RESEARCH_BATCH` 与 Echo/LLM 并存。
- Python 用 `agentflow-rag/.venv`;`.gitignore` 加 `.venv/`、`__pycache__/`。
- 每任务 TDD → commit → push。

---

## File Structure

**agentflow-rag/**(新,Python)
- `app/__init__.py`、`app/embedding.py`、`app/store.py`、`app/main.py`
- `tests/test_embedding.py`、`tests/test_store.py`、`tests/test_recall.py`、`tests/data/labeled.json`
- `requirements.txt`、`run.sh`(启动脚本)

**agentflow-worker/**
- `rag/RagClient.java`、`rag/RagProperties.java`、`rag/Hit.java`
- `processor/ResearchProcessor.java`(type=RESEARCH_BATCH)
- `skill/RagSearchSkill.java`
- `application.yml` 加 `rag.base-url`

**agentflow-server/**
- `coordinator/ResearchBatchDecomposer.java`(type=RESEARCH_BATCH)

---

### Task 1: Python RAG 服务(embedding + pgvector + FastAPI)

**Files:** `agentflow-rag/{requirements.txt,run.sh,app/*.py,tests/test_embedding.py,tests/test_store.py}`、`.gitignore`

- [ ] **Step 1:** `.gitignore` 加 `agentflow-rag/.venv/`、`__pycache__/`、`*.pyc`。`requirements.txt` 记依赖。
- [ ] **Step 2:** `embedding.py`:`embed(text)->list[float]`——分词(lower,`re.split(r"[^a-z0-9]+")`)、词哈希 `hash%DIM` 累加、L2 归一化;`DIM=256`。`test_embedding.py`:模长≈1;共享词相似度 > 无关。
- [ ] **Step 3:** `store.py`:`init()`(CREATE EXTENSION vector + 建表)、`upsert(docs)`、`search(query_vec,k)->[(id,text,score)]`(`<=>` 距离,score=1-距离)。连接串从 env `RAG_DB_URL`(默认 localhost:5433)。`test_store.py`(真 pgvector):upsert 后 search 返回最相近的在前。
- [ ] **Step 4:** `main.py`:FastAPI + Pydantic 模型;`/health`、`/rag/ingest`、`/rag/search`;启动时 `store.init()`。
- [ ] **Step 5:** `run.sh`:`.venv/bin/uvicorn app.main:app --port 8000`。跑 pytest(embedding+store)绿。
- [ ] **Step 6:** commit `feat: W7 Python RAG 服务(确定性 embedding + pgvector + FastAPI)`。

---

### Task 2: Recall@K 小标注集 + 指标

**Files:** `agentflow-rag/tests/data/labeled.json`、`tests/test_recall.py`

- [ ] **Step 1:** `labeled.json`:~20 docs(几个主题簇)+ ~6 queries,每 query 标注期望命中的 doc id(同簇)。
- [ ] **Step 2:** `test_recall.py`:灌库 → 每 query search topK → 计算 Recall@K,断言 ≥ 阈值(确定性下先跑出实际值再定阈值,如 ≥0.6)。
- [ ] **Step 3:** 记录实测 Recall@K 数字(进度报告)。commit `test: W7 Recall@K 小标注集 + 检索质量指标`。

---

### Task 3: Java RagClient(HTTP 检索工具)

**Files:** `agentflow-worker/rag/{RagClient,RagProperties,Hit}.java`、`application.yml`、`test/.../rag/RagClientTest.java`

**Interfaces:** `RagClient.search(String query, int topK): List<Hit>`;`Hit{id,text,score}`。JDK HttpClient POST `/rag/search`,短超时;失败抛(调用方决定降级)。

- [ ] **Step 1:** `RagProperties`(`@ConfigurationProperties("rag")`:`baseUrl` 默认 `http://localhost:8000`,`topK` 默认 3,`timeoutMs`)。`application.yml` 加 `rag`。
- [ ] **Step 2:** `Hit` DTO + `RagClient`(HttpClient,Jackson 解析 hits)。
- [ ] **Step 3:** `RagClientTest`:用 JDK `HttpServer` 起个桩返回固定 JSON,验证解析(不依赖真服务,离线可跑)。
- [ ] **Step 4:** `mvn test` 绿。commit `feat: W7 RagClient(worker HTTP 检索工具入口)`。

---

### Task 4: RESEARCH_BATCH 意图驱动 + 多角色处理器

**Files:** `agentflow-worker/processor/ResearchProcessor.java`、`skill/RagSearchSkill.java`、`agentflow-server/coordinator/ResearchBatchDecomposer.java`、测试

**Interfaces:** `ResearchProcessor implements SubtaskProcessor`(type=`RESEARCH_BATCH`)。input `{question,model?}`;意图门控命中才 `ragClient.search` → 注入 `AgentContext` → `LlmGateway.chat` → schema 校验;输出 `{answer,retrieved:[ids],usedRag,model,promptTokens,completionTokens}`。

- [ ] **Step 1:** `RagSearchSkill`(intent 含 research/资料/对比/什么是…;outputSchema `{answer:string}`;systemPrompt 引导基于给定资料作答)。
- [ ] **Step 2:** `ResearchProcessor`:意图判断 `needsRetrieval(question)`(启发式:问句/关键词);命中→search→把 hits 文本拼进 userInput 或作 history 注入 AgentContext;调 gateway;校验;组装输出(usedRag=命中与否)。
- [ ] **Step 3:** `ResearchProcessorTest`(mock RagClient + mock Gateway):命中意图→调 RagClient 且 usedRag=true;不命中→不调 RagClient 且 usedRag=false。
- [ ] **Step 4:** `ResearchBatchDecomposer`(server,type=RESEARCH_BATCH):payload `{questions:[...],model?}` fan-out,input `{question,model}`。+ 测试。
- [ ] **Step 5:** `mvn test` 全绿(含既有回归)。commit `feat: W7 RESEARCH_BATCH 意图驱动检索 + 多角色处理器`。

---

### Task 5: E2E 验收 + 文档收尾

**Files:** `scripts/w7-rag-demo.sh`、`README.md`、`docs/backlog.md`、`.superpowers/sdd/progress-w7.md`(本地)

- [ ] **Step 1:** `w7-rag-demo.sh`:灌库(几条 doc)→ 提交 `RESEARCH_BATCH`(一条需检索、一条不需)→ 轮询完成 → 打印结果(retrieved ids / usedRag)+ trace + usage。
- [ ] **Step 2:** 端到端手动验证(pgvector + RAG 服务 + server + 1 worker,mock LLM):命中项 usedRag=true 且 retrieved 非空;不命中项 usedRag=false。贴实际输出进报告。
- [ ] **Step 3:** README 加 "## RAG 检索与多角色 Agent(W7)"+ pgvector `docker run` 说明 + compose 片段(记录不改文件)+ 路线图勾选。backlog 记 W7 交付清单 + 自检 + 新发现。
- [ ] **Step 4:** 里程碑自检:① 为何独立 Python 微服务而非 Java 内做 embedding?② 意图驱动检索怎么做、为何不是每步都查?③ pgvector 选型理由(polyglot)?④ Recall@K 怎么测、确定性 embedding 的取舍?
- [ ] **Step 5:** commit `feat: W7 RAG 端到端演示 + 里程碑验收(README/backlog/进度)`。

---

## Self-Review
- **Spec 覆盖**:A(服务)=T1;B(Recall@K)=T2;C(RagClient)=T3;D(意图/多角色)=T4;验收=T5。全覆盖。
- **类型一致**:`RagClient.search(query,topK):List<Hit>`、`ResearchProcessor.{type,process}`、`embed(text)->vec`、`search(vec,k)->hits`——前后一致。
- **需实现者决断**:(a) 确定性 embedding 默认,真实为 backlog 开关;(b) pgvector 端口 5433 经 docker run;(c) 意图判断用启发式(LLM 判意图列 backlog);(d) 多角色=子任务内角色流水,不做 DAG 调度。均已在任务内明确。
- **风险**:跨语言 E2E 需同时起 RAG 服务;Java 侧全部 mock 单测保证离线可测。
