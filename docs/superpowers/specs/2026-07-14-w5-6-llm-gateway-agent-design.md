# W5–6 LLM 网关 + 可控 Agent · 设计文档(子计划三)

> 创建:2026-07-14 · 状态:定稿(基于 master 设计文档已定规格)
> 上游:`docs/specs/2026-07-11-agentflow-design.md`(卖点3 LLM 网关 + AgentContext + Skill 注册)
> 前置:W1–2 骨架 + W3 可靠性 + W4 可观测/Policy 已在 `main`(commit 083d332),全库 48 测试绿
> 执行:直接在 `main` 分支

---

## 0. 定位

W5–6 里程碑:**限流 / 成本管控 / 可控 Agent**。把 W1–4 那个 echo pipeline 升级成"真的调 LLM 的
Agent",同时把大厂面经里"可控可运营 workflow agent"的四个考点落成代码:

| 面经考点(`docs/interview-signals.md`) | 本里程碑落地 |
|---|---|
| 上下文注入 / 防上下文膨胀(腾一 Q5、Q6) | **AgentContext**:组装 prompt + 裁剪历史/输入,超阈值截断 |
| Skill 动态加载 + 输出稳定(腾一 Q9、Q10) | **Skill 注册表 + schema 约束输出**(不让模型自由发挥) |
| 多 skill 命中排序(腾二 Q8) | **命中排序**(priority + 意图匹配) |
| 令牌桶限流 / 成本管控(卖点3) | **LLM 网关**:Redis+Lua 令牌桶 + 多模型路由 + token 记账 |
| 安全/副作用代码层 enforce(腾二 Q6、Q10) | LLM 调用**只能经网关**、副作用只能经 W4 Policy(结构性收口,非提示词) |

**核心设计选择(自主决策,供 review)**:开发/测试**不烧钱、不需要 API key**。LLM 调用抽象成
`LlmClient`,默认注入 `MockLlmClient`(确定性输出,压测/单测都用它);真实模型
(qwen-turbo / gpt-4o-mini 级,OpenAI 兼容协议)作为**配置开关**的第二实现,`llm.provider=mock|openai`。
这正是 master 设计文档第 149 行"压测用 mock LLM(测调度不烧钱)"的落地——网关的限流/路由/记账
逻辑对 mock 和真实模型完全一致,mock 只是把"真的发 HTTP"换成"本地确定性返回",不影响任何被
考核的工程能力。

---

## 1. 范围

| # | 特性 | 一句话 | 卖点/考点 |
|---|------|--------|------|
| A | **LLM 网关** | 所有 LLM 调用经同一闸:Redis+Lua 令牌桶限流 + 多模型路由 + token 记账 | 卖点3 |
| B | **真 Agent(替换 echo)** | 新任务类型 `LLM_BATCH`,worker 经网关调真模型;处理器按 type 路由 | 可控 Agent |
| C | **AgentContext** | prompt 组装 + 历史/输入裁剪,防上下文膨胀 | 腾一 Q5/Q6 |
| D | **Skill 注册 + schema 约束 + 排序** | 工具动态注册,输出 schema 校验,多命中排序 | 腾一 Q9/Q10、腾二 Q8 |
| E | **W4 遗留:通知改 afterCommit** | 副作用移出主事务,避免"对未提交状态提前通知" | W4 defer |

**不在范围**:真 RAG 检索(W7);多角色 Agent 分工(W7);压测数字(W8);ZK(W9)。
`ECHO_BATCH` **保留**(回归 + mock 对照),不删。

---

## 2. LLM 网关设计(卖点3)

### 架构:所有 LLM 调用经同一出口

```
worker Agent(LlmProcessor)
        │  只能调 gateway.chat(request)——拿不到 LlmClient 本身
        ▼
┌─────────────── LlmGateway(唯一出口)───────────────┐
│  ① 路由:按 model 名选 provider + 限流桶            │
│  ② 限流:Redis+Lua 令牌桶(每模型 QPS)——原子扣减  │
│       扣不到令牌 → 抛 RateLimitedException(交重试) │
│  ③ 调用:LlmClient.chat(mock 或 openai)            │
│  ④ 记账:prompt/completion tokens + 估算成本 → Redis│
└────────────────────────────────────────────────────┘
        ▼
   LlmClient(接口) ── MockLlmClient(默认) / OpenAiLlmClient(开关)
```

**为什么要网关(而非各 Agent 直连 LLM SDK)**——对应面经"所有副作用/调用经同一套 policy":
- **限流收口**:并发子任务会同时打 LLM,必须有全局令牌桶削峰,否则触发上游 429 / 烧钱失控。
  桶在 Redis(跨 worker 实例共享),不是 JVM 本地——多 worker 水平扩展时限流才是全局的。
- **成本可观测**:token 记账集中在一处,能答"幂等去重省了多少 token"(master 指标3、6)。
- **可替换**:mock ↔ 真模型、单模型 ↔ 多模型路由,业务代码零改动。
- **代码层 enforce**:Agent 只拿到 `LlmGateway` bean,拿不到底层 client,没有旁路。

### 令牌桶:Redis + Lua(卖点3 硬核点)

- **为什么 Lua**:令牌桶的"读当前令牌 → 按时间补充 → 判断够不够 → 扣减 → 回写"是 check-then-act,
  多 worker 并发下必须原子。Redis 单线程执行整段 Lua 脚本 = 天然原子,免分布式锁。
  (与 W3 状态机用 MySQL CAS 同源思路:把 read-modify-write 压成一个原子操作。)
- **算法**:标准令牌桶。key=`agentflow:ratelimit:<model>`,存 `{tokens, lastRefillMs}`。
  每次请求按 `(now-lastRefill)/1000 * refillPerSec` 补充令牌(上限 capacity),够则扣 1 返回 true。
- **参数**:每模型 `capacity`(桶容量,应对突发)、`refillPerSec`(稳态 QPS),配置在 `llm.models[]`。
- **扣不到令牌**:`LlmGateway.chat` 抛 `RateLimitedException`;worker 侧当作普通失败交给 W3 的
  `RetryRouter` 走阶梯重试(限流是暂时的,重试后大概率能过)——复用已有可靠性层,不另造机制。

### 多模型路由

- `LlmModelProperties`:`llm.models` 列表,每项 `{name, provider, capacity, refillPerSec, pricePer1kTokens}`。
- `ChatRequest` 带 `model`(可空→用默认模型)。`LlmGateway` 按 name 查配置 → 选桶 + 选 client。
- 演示级:一个 mock 模型 + 一个"贵模型/便宜模型"配置项即可展示路由 + 差异化限流/计价,不搭真多家。

### Token 记账

- `TokenUsage{promptTokens, completionTokens, model}` 由 client 返回(mock 用字数估算,真实用 API usage)。
- 网关每次调用后:`INCRBY agentflow:tokens:<model>:prompt` / `:completion`(Redis 原子累加),
  并按 `pricePer1kTokens` 估算成本累加。
- 查询 API:`GET /api/v1/llm/usage` → 各模型累计 token + 估算成本(简历指标3/6 的数据来源)。

---

## 3. 真 Agent(替换 echo)

### 任务类型 `LLM_BATCH` + 处理器按 type 路由

- 新增 decomposer `LlmBatchDecomposer`(type=`LLM_BATCH`):payload `{items:[...], model?}`,
  逐项 fan-out(复用 `SubtaskDef`,input=`{text, model}`),与 `ECHO_BATCH` 同结构。
- worker 侧引入 `SubtaskProcessor` 接口(`type()` + `process(inputJson)`),`EchoProcessor` 与新
  `LlmProcessor` 各实现一个;`SubtaskListener` 按 `msg.getType()` 从注册表选处理器
  ——**顺手修 backlog T4b**(超时重投硬编码 `ECHO_BATCH` 的隐患:引入第二种类型即触发)。
- `LlmProcessor`:用 `AgentContext` 组装 prompt → `LlmGateway.chat` → 拿输出 → 经 `SkillRegistry`
  的 schema 校验 → 返回结构化 JSON(`{summary, model, promptTokens, completionTokens}`)。

### Harness / Runtime 边界(面经腾二 Q4)

`LlmProcessor`(worker 内)= **Runtime**:只负责"想和做"(组 prompt、调模型、产出)。
限流、记账、副作用一律回 **Harness**:限流/记账在 `LlmGateway`(worker 侧但走共享 Redis,属管控),
真正的副作用(通知/建 case)在 server 经 W4 `SideEffectPolicy`。呼应 master"Runtime 只想和做,
副作用回 Harness 经 Policy"。

---

## 4. AgentContext 上下文管理(腾一 Q5/Q6)

### 核心:控制进模型的 token 量,防上下文膨胀

```
AgentContext.build(systemPrompt, userInput, history?)
        │  ① 拼:system + (裁剪后的 history) + userInput
        │  ② 裁剪:总估算 token 超 maxContextTokens → 丢最老的 history / 截断超长 input
        │  ③ 记录:裁剪掉多少条 / 多少 token(可观测,答"如何避免过多历史进模型")
        ▼
   List<ChatMessage> → 交给 LlmGateway
```

- **膨胀防线**:`maxContextTokens` 配置项;token 估算用简单启发式(`chars/4` 近似,English/中文各给系数,
  留校准注释——真实 tokenizer 是 backlog)。超限时**优先丢最老历史**,历史丢光仍超则**截断 userInput**
  (保留头尾,中间省略),绝不无上限塞给模型。
- 本里程碑 echo/单轮 LLM 无多轮 history,`history` 传空即可;裁剪逻辑对"超长单条 input"同样生效
  (演示场景:一篇很长的文档摘要),故功能是真实有用的,不是占位。
- **面经硬答案**:"上下文注入怎么做" = `AgentContext.build` 统一组装;"如何避免过多历史进模型" =
  按 token 预算裁剪,丢老留新 + 截断超长,且裁剪量可观测。

---

## 5. Skill 动态注册 + schema 约束 + 多命中排序(腾一 Q9/Q10、腾二 Q8)

### 核心:输出稳定不靠提示词,靠 schema 校验;工具注册靠 Spring 收集

```
SkillRegistry(Spring 自动收集所有 Skill bean)
    ├─ 动态注册:实现 Skill 接口 + @Component 即自动进注册表(无需改注册代码)
    ├─ 多命中排序:matches(intent) 命中多个 → 按 priority() 降序取第一(或返回有序列表)
    └─ schema 约束:Skill 声明 outputSchema(JSON Schema 子集)
                    → validateOutput(json):不符合 → 抛/重试,不让模型自由发挥
```

- **Skill 接口**:`name()` · `priority()` · `matches(intent)` · `outputSchema()` · `describe()`。
- **动态加载(面经"Skills 怎么动态加载")**:靠 Spring 把所有 `Skill` 实现注入
  `List<Skill>`——新增技能只要加个 `@Component` 类,注册表自动纳入,零配置改动。
- **输出稳定(面经"怎么保证 Skill 产出内容稳定,而不是模型自由发挥")**:每个 Skill 带
  `outputSchema`,LLM 产出必须过 `SchemaValidator.validate(output, schema)`;不合规 → 视为处理失败,
  交 W3 重试(重试时 prompt 可强调 schema)。**硬答案**:结构约束是代码层校验,不是"求"模型听话。
- **多命中排序(面经"多个 skill 同时命中怎么排序")**:`priority()` 数值排序(高优先在前),
  平手用 name 稳定排序。演示两个 skill(如 `summarize` / `extract`)对同一 intent 命中,展示排序。
- **本里程碑集成点**:`LlmProcessor` 用 `LLM_BATCH` 的默认 skill(`summarize`)的 schema 校验 LLM 输出,
  跑通"注册 → 选 skill → 约束输出"完整链路。schema 校验用**手写最小校验器**(必填字段 + 类型),
  不引入 everit-json-schema 之类重依赖(YAGNI:演示够用,完整 JSON Schema 是 backlog)。

---

## 6. W4 遗留:通知改 afterCommit(E)

W4 评审 must-fix(见 `docs/backlog.md` 第 160 行):`NotificationService` 当前在 `TaskFinalizer.aggregate`
里 `updateById` 之后**同一事务内(pre-commit)**执行。接真外部副作用前必须改成
`TransactionSynchronizationManager.registerSynchronization(afterCommit)`——只有终态**真正提交**后才发通知,
避免"对未提交/可能回滚的状态提前通知"。若无活动事务(理论上 finalize 都在事务内,防御性兜底)则直接执行。
W4 Policy 的业务幂等(`notify:<uuid>`)保持不变,afterCommit 只改"时机"不改"去重"。

---

## 7. 任务顺序(TDD,每任务 commit+push)

```
T1 LLM 网关基础:LlmClient 抽象 + MockLlmClient + ChatRequest/Response + TokenUsage + 配置
T2 令牌桶限流(Redis+Lua)+ 多模型路由 + token 记账 + LlmGateway 收口 + /llm/usage
T3 真 Agent:SubtaskProcessor 接口 + LlmProcessor + LlmBatchDecomposer + 处理器按 type 路由(修 T4b)
T4 AgentContext:build + 按 token 预算裁剪(丢老历史 / 截断超长 input)
T5 Skill 注册表 + 手写 schema 校验 + 多命中排序;接入 LlmProcessor 输出校验
T6 W4 遗留:通知改 afterCommit
T7 端到端验收:LLM_BATCH 跑通(mock)+ 限流/记账/schema 演示;README+backlog+里程碑自检
```

## 8. 测试策略

- **单元**:令牌桶(补充/扣减/耗尽);AgentContext 裁剪(不超限不动 / 超限丢老 / 截断超长);
  SchemaValidator(合规/缺字段/类型错);SkillRegistry 排序(多命中按 priority)。
- **集成(真 Redis)**:LlmGateway 限流(打爆桶 → RateLimitedException)+ 记账(token 计数累加);
  处理器 type 路由(ECHO_BATCH→Echo、LLM_BATCH→Llm)。
- **E2E(mock LLM,无需 key)**:提交 `LLM_BATCH` → 各子任务经网关 → trace 完整
  `SUBMITTED→…→TASK_FINALIZED`;`GET /llm/usage` 见 token 累计;schema 不合规 → 重试可见。
- **不烧钱**:全部测试用 `MockLlmClient`,真 `OpenAiLlmClient` 仅手动开关验证(不入 CI)。

## 9. 与上游对齐

- 落实 master 卖点3(LLM 网关)+ AgentContext + Skill 注册(master 第 137–138 行的两个 🆕 组件)。
- 优先级(master 第 211–212 行:Run Trace > Policy > AgentContext > Skill 注册):Run Trace/Policy 已在 W4;
  本里程碑按 网关 > 真 Agent > AgentContext > Skill 顺序,若时间紧,Skill 可留最小实现。
- Harness/Runtime 边界(master 图):网关/记账/限流属 Harness(管控),LlmProcessor 属 Runtime。

## 10. 风险与纪律

| 风险 | 兜底 |
|---|---|
| LLM 费用失控 | 默认 `MockLlmClient`;真实 client 仅配置开关 + 手动验证;网关本身有令牌桶 + 记账 |
| 范围膨胀(Agent 内循环易膨胀成 LangGraph) | 单轮调用即可演示;多轮/工具循环留 W7;Skill schema 用手写最小校验器不引重库 |
| token 估算不准 | 启发式 `chars/系数` + 校准注释;真 tokenizer 列 backlog(演示/限流够用) |
| 令牌桶时钟/精度 | Lua 内用 Redis `TIME`(服务端时钟,免跨节点漂移);capacity 应对突发 |
| 直接在 main | 每任务 TDD + 频繁提交;mock 默认保证测试可离线跑 |
