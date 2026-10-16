# Research Agent 升级设计（RESEARCH_AGENT）

日期：2026-10-15 · 状态：待审阅

## 0. 目标与成功标准

**为什么做：** 现有 AgentFlow 的可靠性层（Kafka 扇出、CAS 状态机、分级重试 + DLQ、超时兜底、幂等、LLM 网关、Run Trace）是真的，但跑在上面的「agent」只是单次 LLM 调用 + 正则门控 RAG，默认还是 mock。打开控制台的人看不出它在做什么。

**做完后应该是：**
1. 用户在页面输入一个研究问题 → 看到问题被拆成 3~4 个子问题 → 每个子问题由一个 agent 在 worker 池上并行跑 ReAct 循环（思考 → 调工具 → 看结果），每一步实时出现在页面上 → 最后得到一篇带 `[n]` 引用、来源可点击的报告。
2. 中途 `docker kill` 一个 worker，页面显示该 agent 被另一个 worker 接手并「从第 N 步续跑」，已调用过的工具不重复调用。
3. 别人 clone 仓库后 `cp .env.example .env && docker compose up -d` 即可跑起全栈；不填 key 用 mock 剧本也能完整走一遍。
4. CI 绿灯徽章；英文 README 默认展示；中文面试讲解文档能让作者逐文件讲清设计。

**约束：** 代码由 Claude 编写；作者需读懂讲解文档。云端 LLM 走现有 OpenAI 兼容网关（DeepSeek / Qwen / OpenAI 等，部署时配置），mock 保留给单测/压测/CI。不加鉴权（仅 tailnet 内访问），README 写明公网暴露前必须加。

## 1. 整体流程：plan → map → reduce

新任务类型 `RESEARCH_AGENT`，payload `{ "question": "...", "model": 可选 }`。一个任务分三个阶段，**每个阶段的每个子任务都是一条普通的 Kafka 子任务消息**，复用全部现有可靠性机制：

| 阶段 | 子任务 kind | 数量 | worker 做什么 | 输出 |
|---|---|---|---|---|
| ① | `PLAN` | 1 | LLM 把问题拆成子问题 | `{subQuestions: [string]}`（2~4 个，SchemaValidator 校验） |
| ② | `AGENT` | N | ReAct 循环（§2） | `{answer, sources:[{title,url}], usage}` |
| ③ | `SYNTHESIZE` | 1 | LLM 把 N 份小结合成报告 | `{report, sources:[{n,title,url}], usage}` |

**所有 LLM 调用都在 worker 经 `LlmGateway`**（限流 + token 记账）；server 不调用模型。

### 1.1 子任务 kind

- `subtask` 表新增可空列 `kind VARCHAR(16)`。`SubtaskDispatcher` 构造 `SubtaskMessage` 时 `type = sub.kind != null ? sub.kind : task.type`，旧任务类型行为不变。
- worker 侧新增 3 个 `SubtaskProcessor`：`PlanProcessor`（type=`PLAN`）、`AgentProcessor`（`AGENT`）、`SynthesizeProcessor`（`SYNTHESIZE`），按现有策略模式自动注册。
- `init.sql` 加列；家里已部署的库执行一次 `ALTER TABLE subtask ADD COLUMN kind VARCHAR(16) NULL`（写进 README 升级说明）。

### 1.2 阶段推进

- `TaskDecomposer` 接口加默认方法 `default List<SubtaskDef> nextStage(TaskEntity task, List<SubtaskEntity> subs) { return List.of(); }`（`SubtaskDef` 增加 `kind` 字段）。只有 `ResearchAgentDecomposer` 覆盖它：
  - 全部已落定的子任务只有 `PLAN` 且成功 → 返回 N 个 `AGENT`（seq 1..N，input = `{question: 子问题, model}`）。
  - `AGENT` 全部落定且至少 1 个成功 → 返回 1 个 `SYNTHESIZE`（seq N+1，input = 原问题 + 每个成功 agent 的 `answer/sources`）。
  - 其他情况（PLAN 失败、AGENT 全败、SYNTHESIZE 已落定）→ 空列表，正常进入终态。
- `ResultHandleService.handle`（已有 `@Transactional`）在累加计数之后、`finalizeIfAllSettled` 之前：若 `done+failed==total`，调用 `nextStage`；非空则**在同一事务内**插入新子任务（PENDING）并 `subtask_total += n`，事务提交后（`afterCommit`）分发；此时 `finalizeIfAllSettled` 因 total 已增大而不会提前落定。
- **并发正确性：** 计数累加是对 task 行的 `UPDATE`，持行锁直到事务提交；多个 AGENT 结果并发到达时串行化，只有最后一个看到 `done+failed==total`，因此 SYNTHESIZE 只会生成一次。
- `SubtaskDispatcher` 抽出 `dispatchPending(TaskEntity)`（不做 PENDING→RUNNING 的 task CAS），供提交时和阶段推进时共用。
- 终态语义不变：SYNTHESIZE 失败 → PARTIAL_FAILED，各 agent 小结仍在 `task.result` 中可见。`TaskFinalizer` 不改。

## 2. ReAct 循环与崩溃续跑

### 2.1 LLM 消息模型扩展（`agentflow-common`）

- `ChatMessage`：新增 `toolCalls`（`List<ToolCall>`，assistant 用）、`toolCallId`（role=`tool` 用）。
- 新增 `ToolCall { id, name, argumentsJson }`、`ToolSpec { name, description, parametersJsonSchema }`。
- `ChatRequest` 新增 `tools`（可空）；`ChatResponse` 新增 `toolCalls`。
- `OpenAiLlmClient` 按 OpenAI `chat/completions` 的 `tools` / `tool_calls` / `role:tool` 格式序列化与解析。
- `MockLlmClient`：请求带 tools 时按剧本返回——第 1 次调用 `wiki_search`，第 2 次调用 `web_fetch`（URL 取自上一步结果），第 3 次给出最终答案；不带 tools 时行为与现在一致。PLAN / SYNTHESIZE 在 mock 下返回固定合法 JSON。

### 2.2 循环（`AgentLoop`，由 `AgentProcessor` 调用）

```
messages = 续跑存档 ?: [system, user(子问题)]
for step = 已完成步数+1 .. MAX_STEPS(6):
    resp = gateway.chat(messages, tools)
    if resp.toolCalls 为空: return 校验(resp.content)        # 最终答案
    results = 依次执行每个 toolCall（§3）
    messages += assistant(resp) + 每个 tool 结果
    checkpoint.append(step, assistant, results)             # 一步完整结束后才存
    trace AGENT_STEP
超过 MAX_STEPS: 去掉 tools 再请求一次，强制作答
```

- 上下文控制：每个工具结果截断到 2000 字符；复用现有 `AgentContext` 的 token 裁剪。
- 最终答案 JSON `{answer, sources}`，经 `SchemaValidator`；引用校验见 §3.3。
- 输出附 `usage`（本次循环所有 LLM 调用的 prompt/completion tokens 与成本，单位微美元），用于页面页脚。

### 2.3 存档与续跑（`AgentCheckpoint`，Redis）

- key：`agent:ckpt:{subtaskUuid}`，Redis list，每步 `RPUSH` 一条 JSON `{step, assistant, toolResults}`，TTL 24h。worker 不连 MySQL（server 是唯一状态写入方），存档只是续跑缓存，不是业务状态。
- 启动时 `LRANGE` 读出全部步骤，重建 messages，从下一步开始，并发 trace `RESUMED`（detail 含 `fromStep`）。
- 触发续跑的三条路径：① worker 崩溃 → offset 未提交 → 消费组再平衡后另一 worker 重新消费；② 循环中 LLM 调用失败（含限流）→ 现有重试 topic → 重新执行时自动续跑；③ 前两条都失效 → 现有 TimeoutSweep 重派。
- 边界：在「工具执行完」与「写存档」之间崩溃，该步重做。工具全部只读，重做无副作用 → at-least-once + 只读工具，效果上等价 exactly-once。
- worker `spring.kafka.listener.concurrency: 2`；默认 `max.poll.interval.ms`(300s) 大于单个 agent 循环的预期耗时（6 步 × ~15s），README 注明调大步数时需同步调整。

## 3. 工具

### 3.1 接口

`AgentTool { String name(); String description(); String parametersSchema(); String run(JsonNode args) throws Exception; }`，`@Component` 自动收集，`AgentLoop` 按 name 建索引并生成 `ToolSpec` 列表。与现有 `Skill`（提示词 + 输出 schema）概念不同，二者并存。

| 工具 | 参数 | 实现 |
|---|---|---|
| `wiki_search` | `query` | Wikipedia `w/api.php?action=query&list=search` 取前 3 条 + REST `page/summary/{title}` 取摘要；按 Wikimedia 政策设置 User-Agent；返回 `title / summary / url` |
| `web_fetch` | `url` | JDK HttpClient GET，去 HTML 标签取正文前 2000 字符 |
| `kb_search` | `query` | 现有 `RagClient.search`，返回片段 + 来源 |

### 3.2 防护

1. **SSRF**：`web_fetch` 只允许 http/https；解析域名后拒绝回环、私网（10/8、172.16/12、192.168/16）、链路本地（169.254/16）、`0.0.0.0` 及 IPv6 对应地址；不跟随重定向到被拒地址（手动跟随 ≤3 次，每跳都校验）；响应上限 1MB，超时 10s。
2. **工具错误不抛出**：未知工具、参数非法、超时、HTTP 错误 → 以 `ERROR: ...` 文本作为工具结果回给模型；只有 LLM 调用本身失败才让子任务进入重试。
3. **重复调用去重**：同一次运行内（含续跑恢复的步骤）相同 `name + 参数` 直接返回缓存结果。
4. **防编造引用**：最终 `sources[].url` 必须出现在本次运行工具真实返回过的 URL 集合里；不在集合内的来源被删除，并发 trace `CITATION_DROPPED`。SYNTHESIZE 同理，只能引用各 agent 输出里的来源。

## 4. 控制台

`index.html` 保持单文件原生 JS，分两个标签，默认 **Research**：

- 顶部一句话说明系统做什么 + 「中途杀 worker 会被接手续跑」。
- 问题输入框 + 3 个示例问题按钮（点击即提交）。
- ① 拆题：显示子问题列表。② Agents：每个子问题一张卡片，显示 workerId、状态，逐步列出 🤔 想法 / 🔧 工具(参数) / 📄 结果摘要；出现 `RESUMED` 时显示「⚡ worker-x → worker-y，从第 N 步续跑」。③ 报告：按段落渲染纯文本，`[n]` 渲染为跳转到来源列表的链接；来源可点击。
- 页脚：LLM 调用次数、总 tokens、估算成本（汇总各子任务输出的 `usage`）。
- **Ops** 标签：原有任务列表 / trace 时间线 / token 统计 / DLQ 视图原样保留。
- 数据：每秒轮询现有任务与 trace 接口。worker 新发 trace 阶段 `AGENT_STEP`、`RESUMED`、`CITATION_DROPPED`（加到 `TraceStage`），`detail` 为 JSON `{step, thought, tool, args, resultPreview, workerId, resumedFrom, tokens}`。workerId 取容器主机名。
- 所有模型/网页来的文本以 `textContent` 渲染，不用 `innerHTML`（防 XSS）。

## 5. 工程门面

1. **Dockerfile ×3**：server、worker（多阶段：maven 构建 → JRE 17 运行）、rag（python slim）。
2. **docker-compose.yml** 补全：mysql、redis、mongo、kafka、pgvector、rag、server、worker-1、worker-2；健康检查 + `depends_on: condition: service_healthy`；server 暴露 8080。
3. **`.env.example`**：`LLM_PROVIDER=mock`、`OPENAI_BASE_URL`、`OPENAI_API_KEY`、`LLM_MODEL`；`.env` 加入 `.gitignore`。
4. **GitHub Actions** `.github/workflows/ci.yml`：`mvn -B test` + RAG `pytest`；全部 mock，无需 key。README 加徽章。
5. **README**：`README.md` 改为英文（原中文版移到 `README.zh.md`，互链）；首屏：一句话 + demo GIF + 一条启动命令 + 架构图（Mermaid）；Future work：每步一条消息的 durable execution、SSE、通用网页搜索、鉴权。GIF 由作者本机录制，README 留录制步骤。
6. **脚本**：`scripts/e2e-agent.sh`（提交 → 轮询到终态 → 断言报告非空且有来源）；`scripts/chaos-agent.sh`（提交 → 等到某 agent 第 2 步 → `docker kill` 其 worker → 断言出现 `RESUMED` 且任务完成）。

## 6. 测试

| 对象 | 用例 |
|---|---|
| `AgentLoop` | 正常多步得到答案；超过 MAX_STEPS 强制作答；工具出错回传模型不抛；重复调用命中缓存 |
| 续跑 | 预置 2 步存档 → 从第 3 步开始，工具调用次数 0，发出 `RESUMED` |
| 引用校验 | 编造 URL 被删除并发 `CITATION_DROPPED` |
| `web_fetch` | 回环 / 私网 / 链路本地 / 重定向到内网全部拒绝 |
| 阶段推进 | PLAN 成功 → 生成 N 个 AGENT 且 total 增加、任务不落定；AGENT 全落定 → 只生成 1 个 SYNTHESIZE；PLAN 失败 → FAILED；AGENT 全败 → FAILED 不合成 |
| `OpenAiLlmClient` | 用固定 JSON 响应验证 tool_calls 解析与请求序列化（本地 HttpServer 桩） |
| 端到端 | `e2e-agent.sh`、`chaos-agent.sh`（本地 / 家里机器手动跑，不进 CI） |

现有 35 个测试与现有 demo 脚本须继续通过。

## 7. 文档

- `docs/interview-walkthrough.md`（中文）：按「一个请求从提交到出报告」的顺序逐文件讲：做了什么 / 为什么这样设计 / 面试可能怎么追问 / 怎么答；附 30 秒与 2 分钟口述稿。
- 更新 `docs/resume-bullets.md`，并提醒同步中英文简历。

## 8. 不做（YAGNI）

鉴权、SSE/WebSocket、每步一条 Kafka 消息的 durable execution（方案 B）、通用搜索引擎工具、Markdown 渲染库、worker 连 MySQL。
