# W5–6 LLM 网关 + 可控 Agent Implementation Plan(子计划三)

> **For agentic workers:** 每任务 TDD → commit → push。Steps 用 checkbox(`- [ ]`)。

**Goal:** 给 AgentFlow 加 LLM 网关(Redis+Lua 令牌桶限流 + 多模型路由 + token 记账)、真 Agent
(经网关调 mock/真模型,替换 echo)、AgentContext(上下文裁剪)、Skill 注册(动态注册 + schema 约束 +
多命中排序);并处理 W4 遗留(通知改 afterCommit)。

**Architecture:** 见 `docs/superpowers/specs/2026-07-14-w5-6-llm-gateway-agent-design.md`。
所有 LLM 调用经 `LlmGateway`(唯一出口)→ 令牌桶 → `LlmClient`(默认 `MockLlmClient`,配置开关切真模型)→
token 记账。worker `SubtaskListener` 按 `msg.getType()` 选 `SubtaskProcessor`(Echo/Llm)。

**Tech Stack:** Java 17 · Spring Boot 3.3.5 · spring-kafka · Redisson(已在 worker+server)· JUnit5+Mockito。
**不新增重依赖**:LLM 真实调用用 JDK `HttpClient` 走 OpenAI 兼容协议(mock 默认,真实为开关);schema 用手写校验器。

## Global Constraints

- **直接在 `main` 分支**;当前 `main` @ `083d332`,全库 48 测试绿。
- **LLM 默认 mock**(`llm.provider=mock`):所有测试离线可跑、不烧钱、无需 API key。
- **LLM 调用只能经 `LlmGateway`**(代码层 enforce:Agent 只注入 gateway,拿不到 LlmClient)。
- **令牌桶在 Redis(Lua 原子)**,跨 worker 共享——多实例限流才是全局的。
- 网关组件放 **worker 模块**(调用方在 worker);usage 查询 API 在 server(读同一 Redis)。
- 处理器路由**不改既有 ECHO_BATCH 语义**;新增 LLM_BATCH 与之并存。
- 每任务 TDD → commit → push。集成测试直连真实 Redis(已起)。Docker 命令前 `export PATH="$HOME/.orbstack/bin:$PATH"`。

---

## File Structure

**agentflow-common**
- `llm/ChatMessage.java`、`llm/ChatRequest.java`、`llm/ChatResponse.java`、`llm/TokenUsage.java`(DTO,供 worker+server 共用记账查询)。

**agentflow-worker**
- `llm/LlmClient.java`(接口)、`llm/MockLlmClient.java`、`llm/OpenAiLlmClient.java`(@ConditionalOnProperty)。
- `llm/LlmModelProperties.java`(@ConfigurationProperties `llm`)、`llm/LlmGateway.java`、`llm/RateLimiter.java`(Redis+Lua)、`llm/TokenAccountant.java`、`llm/RateLimitedException.java`。
- `context/AgentContext.java`、`context/TokenEstimator.java`。
- `skill/Skill.java`、`skill/SkillRegistry.java`、`skill/SchemaValidator.java`、`skill/SummarizeSkill.java`(+ 第二个演示 skill)。
- `processor/SubtaskProcessor.java`(接口)、`processor/LlmProcessor.java`;`processor/EchoProcessor.java` 改实现接口。
- `listener/SubtaskListener.java` 改为按 type 选处理器 + 修超时重投 type 硬编码(T4b 在 server 侧)。
- `application.yml` 加 `llm.*`。

**agentflow-server**
- `coordinator/LlmBatchDecomposer.java`(type=LLM_BATCH)。
- `controller/LlmUsageController.java`(GET /api/v1/llm/usage,读 Redis)。
- `service/TaskFinalizer.java` 或 `NotificationService.java` 改 afterCommit(T6)。
- `service/TimeoutSweepService.java`:超时重投用 `task.getType()` 而非硬编码(T4b)。

---

### Task 1: LLM 网关基础(LlmClient 抽象 + MockLlmClient + DTO + 配置)

**Files:**
- Create: `agentflow-common/.../llm/{ChatMessage,ChatRequest,ChatResponse,TokenUsage}.java`
- Create: `agentflow-worker/.../llm/{LlmClient,MockLlmClient,LlmModelProperties}.java`
- Modify: `agentflow-worker/src/main/resources/application.yml`
- Test: `agentflow-worker/.../llm/MockLlmClientTest.java`

**Interfaces:**
- `ChatRequest{model, List<ChatMessage> messages, maxTokens}`;`ChatMessage{role, content}`;
  `ChatResponse{content, TokenUsage usage, model}`;`TokenUsage{promptTokens, completionTokens}`。
- `LlmClient.chat(ChatRequest): ChatResponse`。`MockLlmClient`:确定性——content=拼接输入摘要,
  usage 用字数估算;不发网络。

- [ ] **Step 1:** DTO(手写 getter/setter,同 mq DTO 风格,便于 Kafka/JSON 序列化)。
- [ ] **Step 2:** `LlmClient` 接口 + `MockLlmClient`(`@Component @ConditionalOnProperty(name="llm.provider", havingValue="mock", matchIfMissing=true)`)。
- [ ] **Step 3:** `LlmModelProperties`(`@ConfigurationProperties("llm")`:`provider` + `defaultModel` + `List<Model>{name,capacity,refillPerSec,pricePer1kTokens}`)。`application.yml` 加 `llm`(provider=mock,两个模型)。`WorkerApplication` 加 `@ConfigurationPropertiesScan` 或 `@EnableConfigurationProperties`。
- [ ] **Step 4:** `MockLlmClientTest`——同输入确定性输出、usage>0。
- [ ] **Step 5:** `mvn -q test` 全绿 → commit `feat: LLM 网关基础(LlmClient 抽象+MockLlmClient+DTO+配置)`。

---

### Task 2: 令牌桶限流(Redis+Lua)+ 多模型路由 + token 记账 + LlmGateway

**Files:**
- Create: `agentflow-worker/.../llm/{RateLimiter,RateLimitedException,TokenAccountant,LlmGateway}.java`
- Create: `agentflow-worker/src/main/resources/token_bucket.lua`
- Create: `agentflow-server/.../controller/LlmUsageController.java`
- Test: `agentflow-worker/.../llm/RateLimiterTest.java`、`LlmGatewayTest.java`

**Interfaces:**
- `RateLimiter.tryAcquire(model): boolean`(Redis Lua 原子令牌桶)。
- `TokenAccountant.record(model, TokenUsage)`(Redis INCRBY);`usage(): Map<model, {prompt,completion,costUsd}>`。
- `LlmGateway.chat(ChatRequest): ChatResponse`——路由选桶 → `tryAcquire` 失败抛 `RateLimitedException` →
  选 client → 调用 → 记账 → 返回。**worker Agent 只注入 LlmGateway**。
- `GET /api/v1/llm/usage` → 各模型累计 token + 估算成本。

**令牌桶 Lua**(`token_bucket.lua`,KEYS[1]=bucketKey,ARGV=capacity,refillPerSec,nowMs):
```lua
-- 标准令牌桶:读 tokens/lastRefill → 按时间补充(上限 capacity)→ 够则扣 1
local b = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
local cap = tonumber(ARGV[1]); local refill = tonumber(ARGV[2]); local now = tonumber(ARGV[3])
local tokens = tonumber(b[1]); local ts = tonumber(b[2])
if tokens == nil then tokens = cap; ts = now end
local delta = math.max(0, now - ts) / 1000.0 * refill
tokens = math.min(cap, tokens + delta)
local ok = 0
if tokens >= 1 then tokens = tokens - 1; ok = 1 end
redis.call('HMSET', KEYS[1], 'tokens', tokens, 'ts', now)
redis.call('PEXPIRE', KEYS[1], 60000)
return ok
```
> now 用调用方传入的 `System.currentTimeMillis()`(单 Redis 实例足够;跨节点时钟漂移列 backlog,可改 Redis `TIME`)。

- [ ] **Step 1:** `RateLimiterTest`(真 Redis)——capacity=2 连取 2 次 true、第 3 次 false;等待补充后再 true。先失败。
- [ ] **Step 2:** `RateLimiter`(Redisson `getScript().eval` 或 spring-data-redis execute Lua);`RateLimitedException`。
- [ ] **Step 3:** `TokenAccountant`(Redis INCRBY prompt/completion;usage() 读回 + 按 price 算 cost)。
- [ ] **Step 4:** `LlmGateway`(注入 `List<LlmClient>` 按 provider 选 + RateLimiter + TokenAccountant + props)。`LlmGatewayTest`:mock client + 真 Redis,验证限流打爆抛异常、记账累加。
- [ ] **Step 5:** `LlmUsageController`(server,读同一 Redis 的 TokenAccountant——server 也需一份 TokenAccountant 或把它挪 common;**决策:TokenAccountant 逻辑放 common 或 server 各持一份读同 key**。取简单:usage 读逻辑在 server 直接读 Redis key)。
- [ ] **Step 6:** `mvn -q test` 全绿 → commit `feat: LLM 网关——Redis+Lua 令牌桶限流+多模型路由+token 记账`。

---

### Task 3: 真 Agent(SubtaskProcessor 接口 + LlmProcessor + LlmBatchDecomposer + type 路由)

**Files:**
- Create: `agentflow-worker/.../processor/SubtaskProcessor.java`、`processor/LlmProcessor.java`
- Modify: `agentflow-worker/.../processor/EchoProcessor.java`(实现 SubtaskProcessor,加 `type()="ECHO_BATCH"`)
- Modify: `agentflow-worker/.../listener/SubtaskListener.java`(注入 `Map<String,SubtaskProcessor>` 或 List,按 `msg.getType()` 选)
- Create: `agentflow-server/.../coordinator/LlmBatchDecomposer.java`
- Modify: `agentflow-server/.../service/TimeoutSweepService.java`(超时重投用 `task.getType()`,修 backlog T4b)
- Test: `LlmProcessorTest.java`、`SubtaskListener` 既有测试更新(注入处理器 map)、`LlmBatchDecomposerTest.java`

**Interfaces:**
- `SubtaskProcessor.type(): String` · `process(inputJson): String`。Spring 注入所有实现,按 type 建 map。
- `LlmProcessor`(type=`LLM_BATCH`):`AgentContext.build` → `LlmGateway.chat` → 输出 JSON `{summary,model,promptTokens,completionTokens}`。
- `LlmBatchDecomposer`(type=`LLM_BATCH`):payload `{items:[...],model?}` fan-out,input=`{text,model}`。

- [ ] **Step 1:** `SubtaskProcessor` 接口;`EchoProcessor` 实现它(逻辑不变,加 `type()`)。
- [ ] **Step 2:** `SubtaskListener` 改:注入 `List<SubtaskProcessor>` → `Map<type,processor>`;`onMessage` 用 `msg.getType()` 选;未知 type 抛(交重试/DLQ)。更新既有 `SubtaskListenerTest`/`SubtaskListenerIdempotencyTest` 构造。
- [ ] **Step 3:** `LlmProcessor`(注入 LlmGateway + AgentContext + SkillRegistry——Task4/5 未完成前先只用 gateway,占位 schema 校验 Task5 接)。`LlmProcessorTest`(mock gateway)。
- [ ] **Step 4:** `LlmBatchDecomposer`(server)+ `LlmBatchDecomposerTest`。
- [ ] **Step 5:** `TimeoutSweepService` 超时重投的 `SubtaskMessage.type` 改 `task.getType()`(T4b);确认有 task 可查。
- [ ] **Step 6:** `mvn -q test` 全绿 → commit `feat: 真 Agent(LlmProcessor 经网关)+ 处理器按 type 路由(修 T4b type 硬编码)`。

---

### Task 4: AgentContext 上下文管理

**Files:**
- Create: `agentflow-worker/.../context/AgentContext.java`、`context/TokenEstimator.java`
- Modify: `LlmProcessor` 用 AgentContext 组装
- Test: `AgentContextTest.java`、`TokenEstimatorTest.java`

**Interfaces:**
- `TokenEstimator.estimate(String): int`(`chars/系数` 启发式,中英各系数,校准注释)。
- `AgentContext.build(system, userInput, history): List<ChatMessage>`——总估算超 `maxContextTokens` →
  丢最老 history;history 丢光仍超 → 截断 userInput(保留头尾)。返回结果 + 记录裁剪量(log)。

- [ ] **Step 1:** `TokenEstimatorTest`(空=0、纯英文、含中文的估算范围)。`TokenEstimator`。
- [ ] **Step 2:** `AgentContextTest`——不超限原样;超限丢老历史;历史丢光截断超长 input(头尾保留、中间省略标记)。`AgentContext`。
- [ ] **Step 3:** `LlmProcessor` 接入 `AgentContext.build`。
- [ ] **Step 4:** `mvn -q test` 全绿 → commit `feat: AgentContext 上下文裁剪(按 token 预算防膨胀)`。

---

### Task 5: Skill 注册 + schema 约束 + 多命中排序

**Files:**
- Create: `agentflow-worker/.../skill/{Skill,SkillRegistry,SchemaValidator,SummarizeSkill,ExtractSkill}.java`
- Modify: `LlmProcessor` 用 SkillRegistry 选 skill + 校验输出
- Test: `SchemaValidatorTest.java`、`SkillRegistryTest.java`

**Interfaces:**
- `Skill`:`name()` · `priority():int` · `matches(intent):boolean` · `outputSchema():Map`(字段→类型) · `describe():String`。
- `SkillRegistry.select(intent): List<Skill>`(命中项按 priority 降序、name 稳定)。
- `SchemaValidator.validate(jsonNode, schema): List<String>`(违规原因;空=通过)。手写:必填字段 + 类型(string/number/object)。

- [ ] **Step 1:** `SchemaValidatorTest`(合规通过、缺字段报错、类型错报错)。`SchemaValidator`。
- [ ] **Step 2:** `Skill` 接口 + `SummarizeSkill`(priority 高)+ `ExtractSkill`(priority 低,同 intent 命中);`SkillRegistryTest`——多命中按 priority 排序。`SkillRegistry`(注入 `List<Skill>`)。
- [ ] **Step 3:** `LlmProcessor`:选 skill → 校验 LLM 输出符合 skill.outputSchema;不合规 → 抛(交 W3 重试)。
- [ ] **Step 4:** `mvn -q test` 全绿 → commit `feat: Skill 动态注册+schema 约束输出+多命中排序`。

---

### Task 6: W4 遗留——通知改 afterCommit

**Files:**
- Modify: `agentflow-server/.../service/NotificationService.java`(或 TaskFinalizer 调用处)
- Test: 既有 `NotificationDedupeTest` 保持;加/改断言验证 afterCommit 时机(有事务时注册同步、无事务时直接执行)

**Interfaces:**
- `NotificationService.notifyTaskFinished(task)`:若 `TransactionSynchronizationManager.isSynchronizationActive()`
  → 注册 `afterCommit` 回调里 `policy.execute(...)`;否则直接执行(防御兜底)。业务幂等键不变。

- [ ] **Step 1:** 改 `NotificationService`:包一层 afterCommit。
- [ ] **Step 2:** 测试:无事务上下文调用仍执行一次(现有测试);(可选)有事务且回滚 → 不通知。
- [ ] **Step 3:** `mvn -q test` 全绿 → commit `fix: 任务完成通知改 afterCommit(W4 遗留:避免对未提交状态提前通知)`。

---

### Task 7: 端到端验收 + 里程碑收尾

**Files:**
- Create: `scripts/w5-6-demo.sh`
- Modify: `README.md`(LLM 网关/Agent/Context/Skill 说明 + 路线图勾选)、`docs/backlog.md`

- [ ] **Step 1:** `w5-6-demo.sh`:提交 `LLM_BATCH`(mock)→ 轮询完成 → 打印 result + `GET /trace` + `GET /llm/usage`。
- [ ] **Step 2:** 端到端手动验证(server + 1 worker,mock LLM):LLM_BATCH 跑通,trace 完整,usage 有 token,schema 校验生效(故意坏 schema 看重试)。贴实际输出进报告。
- [ ] **Step 3:** README 加 "## LLM 网关与可控 Agent(W5–6)";路线图勾选 W5–6。backlog 记新发现 + 关闭 T4b、W4 afterCommit 遗留。
- [ ] **Step 4:** 里程碑自检(报告回答):
  1. 令牌桶为何用 Redis+Lua 而非 JVM 本地/分布式锁?
  2. LLM 调用如何代码层 enforce 经网关(非提示词)?
  3. 上下文注入怎么做、如何避免过多历史进模型?
  4. Skill 输出稳定怎么保证(schema vs 提示词)?多命中如何排序?
- [ ] **Step 5:** commit `feat: W5-6 LLM 网关/Agent 端到端演示 + 里程碑验收`。

---

## Self-Review
- **Spec 覆盖**:网关(A)=T1-2;真 Agent(B)=T3;AgentContext(C)=T4;Skill(D)=T5;W4 遗留(E)=T6;验收=T7。全覆盖。
- **类型一致**:`LlmGateway.chat(ChatRequest):ChatResponse`、`RateLimiter.tryAcquire(model):boolean`、`SubtaskProcessor.{type,process}`、`AgentContext.build(...):List<ChatMessage>`、`SkillRegistry.select(intent):List<Skill>`——前后一致。
- **需实现者决断**:(a) 默认 mock,真实 OpenAI client 仅开关;(b) 网关组件在 worker,usage API 在 server 读同 Redis key;(c) schema 用手写最小校验器不引重库;(d) 限流失败复用 W3 重试而非新机制。均已在任务内明确。
- **风险**:`SubtaskListener` 改处理器路由波及既有 worker 测试(T3 回归);默认 mock 保证离线可测。
