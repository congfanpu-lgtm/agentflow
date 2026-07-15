# W7 RAG 检索服务 + 意图驱动 + 多角色分工 · 设计文档(子计划四)

> 创建:2026-07-15 · 状态:定稿(基于 master 设计文档已定规格)
> 上游:`docs/specs/2026-07-11-agentflow-design.md`(RAG 检索服务 + 多角色 Agent 分工 + 意图驱动检索)
> 前置:W1–2 骨架 + W3 可靠性 + W4 可观测 + W5–6 LLM 网关/可控 Agent 已在 `main`(commit 338872c),全库 78 测试绿
> 执行:直接在 `main` 分支

---

## 0. 定位

W7 里程碑:**RAG 工具 + 多 Agent 协作**。给可控 Agent 加一个"检索工具"——一个独立的
**Python/FastAPI 微服务**(embedding + pgvector 近邻检索),Agent **按意图判断决定是否检索**,
把召回结果注入 `AgentContext` 再交 LLM。对应 master 演示场景"分析 N 篇文档生成对比报告"。

| 面经/卖点 | 本里程碑落地 |
|---|---|
| RAG 工具(意图驱动调用) | Agent 判断"这条输入是否需要外部知识"→ 命中才调 RAG,不是每步都查 |
| 跨语言微服务(美国 AIE 友好) | Java(主链路)+ Python/FastAPI(embedding 生态)双语言架构 |
| 多角色 Agent 分工 | 一个子任务内的角色流水:检索(retrieve)→ 分析/汇总(summarize),跨子任务聚合=汇总 |
| polyglot persistence | 再加一种存储 pgvector(向量近邻),按访问模式选存储的成熟度 |
| 检索质量指标(master 指标5) | Recall@K,自建小标注集(几十条) |

**核心设计选择(自主决策,供 review)**:延续 mock-first 哲学——**默认用确定性本地 embedding**
(词哈希投影到固定维向量,离线、可复现、无需 API key、无需下载模型),真实 embedding
(sentence-transformers / OpenAI 兼容)作为 backlog 的配置开关。确定性 embedding 让 Recall@K
与集成测试都能离线跑;共享词越多余弦越高,足以演示"语义相近的召回"与检索链路,不卷复杂 RAG
(master 明确"向量库用现成,链路自搭,不卷复杂 RAG")。

---

## 1. 范围

| # | 特性 | 一句话 |
|---|------|--------|
| A | **Python RAG 服务** | FastAPI:`/rag/ingest`(灌库)+`/rag/search`(ANN 检索)+`/health`;pgvector 存储 |
| B | **确定性 embedding + Recall@K** | 词哈希投影(dim 256,L2 归一化);小标注集测 Recall@K |
| C | **Java RagClient(检索工具)** | worker 经 HTTP 调 RAG 服务(JDK HttpClient,无新依赖) |
| D | **意图驱动检索 + 多角色处理器** | `RESEARCH_BATCH` 类型 + `ResearchProcessor`:意图判断→(命中才)检索→注入 Context→LLM 汇总 |

**不在范围**:真 embedding 模型(backlog 开关);DAG 依赖边调度多 Agent(backlog,现为角色流水);
混合召回的复杂 rerank(只做向量 ANN + 简单关键词兜底);压测(W8);ZK(W9)。
`ECHO_BATCH` / `LLM_BATCH` 保留。

---

## 2. Python RAG 服务(A + B)

### 结构(`agentflow-rag/`,独立微服务)

```
agentflow-rag/
  app/main.py        FastAPI:/health /rag/ingest /rag/search
  app/embedding.py   确定性词哈希 embedding(dim 256)+ 真实 embedding 开关(backlog)
  app/store.py       pgvector:建表/建索引/upsert/ANN 检索(psycopg)
  tests/             embedding 归一化/相似度;Recall@K(小标注集)
  requirements.txt   fastapi uvicorn psycopg[binary] numpy pytest httpx
  .venv/             (gitignore)
```

### API

- `GET /health` → `{"status":"ok"}`(server 侧探活)。
- `POST /rag/ingest` body `{"docs":[{"id":"d1","text":"..."}]}` → embed 每条 + upsert 到 pgvector;返回 `{"ingested":n}`。
- `POST /rag/search` body `{"query":"...","topK":3}` → embed query + cosine ANN → `{"hits":[{"id","text","score"}]}`(score=余弦相似,降序)。

### 确定性 embedding(词哈希投影)

- 分词(小写、按非字母数字切),每个 token `hash(token) % dim` 命中一维,值 +1(可加 sublinear);
  L2 归一化 → 单位向量。**共享词越多余弦越高**——同义/相关文本能召回。
- dim=256(pgvector `vector(256)`)。确定性:同文本永远同向量 → Recall@K 与测试可复现。
- **校准位**:dim、是否 sublinear tf、停用词——留常量 + 注释,真实 embedding 是 backlog 开关。

### pgvector 存储

- 表 `documents(id text primary key, content text, embedding vector(256))`,`CREATE EXTENSION vector`。
- 检索:`ORDER BY embedding <=> %s::vector LIMIT k`(`<=>` = 余弦距离),score=`1 - 距离`。
- 演示规模用精确检索(无需 ivfflat 索引);索引/分片留 backlog。

### Recall@K(master 指标5)

- 自建小标注集(几十条 doc + 若干 query→期望 doc id)。测试计算 `Recall@K = 命中期望/总期望`。
- 记录实测数字(简历指标5 来源),脚本/数据留仓库可复现。

---

## 3. Java 侧集成:意图驱动检索 + 多角色(C + D)

### RagClient(检索工具,worker)

- `RagClient.search(query, topK): List<Hit>`——JDK HttpClient POST `/rag/search`,解析 hits。
- base-url 配置 `rag.base-url`(默认 `http://localhost:8000`),超时短(检索是旁路,失败不拖垮)。
- **工具语义**:RAG 是 Agent 的一个"工具",经 RagClient 唯一入口调用(与 LLM 经网关同构)。

### 意图驱动检索 + 多角色处理器

新任务类型 `RESEARCH_BATCH`,`ResearchProcessor`(Runtime,角色流水):

```
input {question, model?}
  │ ① 意图判断(role: router):这条 question 是否需要外部知识?
  │     命中(疑问/"对比/根据资料/什么是"…)→ 检索;否则跳过(不是每步都查)
  │ ② 检索(role: retriever):命中才 RagClient.search(question, topK) → hits
  │ ③ 组装(AgentContext):把 hits 作为上下文注入(超预算自动裁剪,复用 W5-6)
  │ ④ 汇总(role: summarizer):LlmGateway.chat → 经 skill schema 校验
  ▼ 输出 {answer, retrieved:[ids], usedRag:bool, model, tokens}
```

- **意图驱动(面经"agent 自主决定是否检索")**:`ResearchProcessor` 先判意图,**命中才检索**——
  不是每条都查。判断用轻量启发式(问句/关键词),硬答案是"检索是有成本的旁路工具,由意图门控"。
- **多角色分工**:router→retriever→summarizer 是一条子任务内的角色流水;跨子任务的聚合由既有
  `TaskFinalizer` 完成(=汇总角色)。**不引入 DAG 依赖边调度**(那是 backlog),用角色流水足够演示
  "多角色协作"而不过度设计。
- 处理器按 `msg.type` 路由(复用 W5-6 的 `SubtaskProcessor` 策略模式;Echo/LLM/Research 并存)。
- `RagSearchSkill`:声明检索意图匹配 + 输出 schema(`{answer:string}`),接 SkillRegistry。

### Harness/Runtime 边界

RagClient/意图判断/LLM 调用都在 worker(Runtime,"想和做");RAG 服务本身是外部工具。
检索**不产生副作用**(只读),故不经 Policy;真正副作用(通知)仍回 Harness 经 Policy(W4)。

---

## 4. 任务顺序(TDD,每任务 commit+push)

```
T1 Python RAG 服务:embedding(确定性)+ store(pgvector)+ FastAPI(ingest/search/health)+ pytest
T2 Recall@K:小标注集 + 测试 + 记录数字
T3 Java RagClient(HTTP)+ 配置 + 测试(mock 服务 / 真服务)
T4 RESEARCH_BATCH:ResearchProcessor(意图门控→检索→Context→LLM)+ decomposer + type 路由 + RagSearchSkill
T5 E2E:灌库→提交 RESEARCH_BATCH→命中检索的与不命中的对照;trace + usage;README/backlog/进度/自检
```

## 5. 测试策略
- **Python 单元**:embedding 归一化(模长≈1)、相似度(共享词 > 无关);store upsert/search(真 pgvector)。
- **Python Recall@K**:小标注集,断言 Recall@K ≥ 阈值(确定性 embedding 下可复现)。
- **Java 单元**:RagClient 解析(mock HTTP)或跳过真服务;ResearchProcessor 意图门控(命中→调 RAG、不命中→不调,用 mock RagClient + mock Gateway 验证)。
- **E2E**(mock LLM + 真 RAG 服务 + pgvector):灌库→RESEARCH_BATCH→输出含 retrieved ids;意图不命中的 item 不检索。

## 6. 与上游对齐
- 落实 master:RAG 检索服务(Python/FastAPI + pgvector)+ 意图驱动 + 多角色分工 + polyglot persistence + 指标5。
- 延续 mock-first(确定性 embedding vs mock LLM);工具经唯一 client 入口(RagClient / LlmGateway 同构)。

## 7. 风险与纪律
| 风险 | 兜底 |
|---|---|
| RAG 复杂度膨胀 | 只做 ingest/search;确定性 embedding;向量 ANN + 简单兜底,不做复杂 rerank |
| 跨语言联调成本 | RAG 服务独立可单测(pytest);Java 侧用 mock RagClient 单测,E2E 才连真服务 |
| pgvector 容器 | 单容器 `pgvector/pgvector:pg16`,`docker run`(compose 文件被占用,记录配置不改) |
| embedding 无语义 | 确定性词哈希只有词面相似;真实 embedding 列 backlog 开关,当前够演示 + Recall@K |
| 意图判断过简 | 启发式 + 注释;真实用 LLM 判意图列 backlog(避免每步多一次 LLM 调用) |
| 直接在 main | 每任务 TDD + 频繁提交;默认 mock/确定性保证离线可测 |
