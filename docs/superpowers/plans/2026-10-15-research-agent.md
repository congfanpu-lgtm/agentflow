# Research Agent（RESEARCH_AGENT）实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在现有 Kafka 编排与可靠性层之上实现真正的研究型 agent。用户输入一个问题，系统按「拆题 → 多个 ReAct agent 并行调研（可崩溃续跑）→ 合成带引用报告」执行，页面实时展示每一步；同时补全一条命令启动、CI、英文 README 和面试讲解文档。

**Architecture:** 新任务类型 `RESEARCH_AGENT` 分三个阶段（PLAN → N×AGENT → SYNTHESIZE），每个阶段的子任务都是普通的 Kafka 子任务消息。server 在某阶段全部落定时，于同一事务内生成下一阶段的子任务。worker 内的 `AgentLoop` 跑 OpenAI 原生 tool calling 的 ReAct 循环，每步结束后把存档写进 Redis，重新投递时从下一步续跑。所有 LLM 调用都经 `LlmGateway`。

**Tech Stack:** Java 17、Spring Boot 3.3.5、spring-kafka、MyBatis-Plus、Redisson 3.34.1、Jackson、JDK HttpClient、原生 JS 单文件页面、Docker Compose、GitHub Actions、Python FastAPI（RAG，已有）。

**Spec:** `docs/superpowers/specs/2026-10-15-research-agent-design.md`

## Global Constraints

- 不新增任何 Maven 依赖。HTTP 用 JDK `HttpClient`，JSON 用 Jackson，Redis 用已有的 Redisson。
- 代码注释用中文，风格跟随现有文件（类注释写「做什么 + 为什么」）。
- 所有 LLM 调用只经 `LlmGateway.chat`，业务代码不直接持有 `LlmClient`。
- worker 不连 MySQL：子任务状态只由 server 写入；agent 存档只放 Redis。
- 默认 `llm.provider=mock`。单测、CI 都不需要 API key，也不访问外网（外部 HTTP 一律用本地 `HttpServer` 桩）。
- 常量：`MAX_STEPS = 6`、工具结果截断 `2000` 字符、`web_fetch` 上限 `1MB` / 超时 `10s` / 重定向 `3` 次、存档 TTL `24h`、问题长度上限 `500` 字符、拆题上限 `4` 个。
- 工具全部只读，不产生副作用。
- 页面上所有来自模型或网页的文本都必须按纯文本渲染（`textContent` 或转义），链接只允许 `http://` / `https://`。
- 在 `feat/research-agent` 分支上开发，每个任务结束 commit 一次；**不 push**（push 需要用户明确同意）。
- **不碰家里那台机器**（`100.65.10.56`），Task 13 需要用户确认后才执行。
- Research 页签的界面文案用英文（面向美国招聘方）；Ops 页签保持原有中文。

## Review Focus

1. **网页或模型输出里夹带 HTML/脚本（XSS）。** 期望：Research 和 Ops 两个页签都按纯文本显示，不执行任何脚本。→ Task 8 Step 4 用 devtools 注入恶意 detail 校验。
2. **模型给出 `javascript:` 或 `kb://` 之类的来源 URL。** 期望：只有 http/https 渲染为可点击链接，其他按纯文本显示。→ Task 8 Step 4。
3. **真实模型把最终答案包在 ```json 代码块里，或者直接写一段话不给 JSON。** 期望：代码块能解析；纯文本整段当作答案、来源为空，不进入重试。→ Task 2（`TextUtil`）与 Task 5（`finalAnswerInProseFallsBack`）。
4. **模型给的 tool_calls 参数是空串或非法 JSON。** 期望：以 `ERROR:` 文本回给模型，循环继续，不抛异常。→ Task 5（`malformedArgumentsReturnedAsError`）。
5. **拆题结果为空、重复或超过 4 个。** 期望：空 → 子任务失败并进入重试；重复去掉；超过 4 个截断到 4。→ Task 6（`PlanProcessorTest`）。

---

## 文件结构

**agentflow-common**（`src/main/java/com/agentflow/common/`）
- `llm/ToolCall.java`（新）：模型发起的一次工具调用 `{id, name, argumentsJson}`
- `llm/ToolSpec.java`（新）：提供给模型的工具描述 record
- `llm/ChatMessage.java`、`llm/ChatRequest.java`、`llm/ChatResponse.java`（改）：加 tool calling 字段
- `trace/TraceStage.java`（改）：加 `STAGE_ADVANCED, AGENT_STEP, RESUMED, CITATION_DROPPED`

**agentflow-worker**（`src/main/java/com/agentflow/worker/`）
- `llm/OpenAiLlmClient.java`（改）：tool calling 的序列化与解析
- `llm/MockLlmClient.java`（改）：按系统提示的标签走固定剧本，加可配置延迟
- `agent/Prompts.java`（新）：三个角色的系统提示（以标签开头，mock 据此识别角色）
- `agent/TextUtil.java`（新）：宽松 JSON 提取、URL 提取、截断
- `agent/Usage.java`（新）：token 与成本累计（随输出返回给页面）
- `agent/AgentRun.java`、`agent/AgentStep.java`（新）：record
- `agent/AgentCheckpoint.java`（新）：Redis 存档
- `agent/AgentLoop.java`（新）：ReAct 循环（续跑、去重、防编造引用、trace）
- `agent/tool/AgentTool.java`、`UrlGuard.java`、`WebFetchTool.java`、`WikiSearchTool.java`、`KbSearchTool.java`（新）
- `processor/SubtaskProcessor.java`（改）：加 `default process(SubtaskMessage)`
- `processor/PlanProcessor.java`、`AgentProcessor.java`、`SynthesizeProcessor.java`（新）
- `listener/SubtaskListener.java`（改）：改为调用 `process(msg)`
- `resources/application.yml`（改）：监听并发 2、真实模型条目、mock 延迟

**agentflow-server**（`src/main/java/com/agentflow/server/`）
- `entity/SubtaskEntity.java`（改）：加 `kind`
- `coordinator/SubtaskDef.java`（改）：加 `kind` 与 `toEntity`
- `coordinator/TaskDecomposer.java`（改）：加 `default nextStage`
- `coordinator/ResearchAgentDecomposer.java`（新）
- `coordinator/StageAdvancer.java`（新）：子任务落定后的唯一入口（推进阶段或落定终态）
- `coordinator/SubtaskDispatcher.java`（改）：拆出 `dispatchPending`、`typeOf`
- `mapper/TaskMapper.java`（改）：加 `addTotal`
- `service/ResultHandleService.java`、`TimeoutSweepService.java`（改）：改为调用 `stageAdvancer.onSettled`；超时与 DLQ 重投用 `typeOf`
- `service/DlqRecoveryService.java`、`TaskSubmitService.java`、`TaskFinalizer.java`（小改）
- `resources/static/index.html`（改）：Research / Ops 两个页签

**部署 / 文档**
- `docker/mysql/init.sql`（改）、`docker/mysql/migrations/2026-10-15-subtask-kind.sql`（新）
- `docker/java.Dockerfile`、`agentflow-rag/Dockerfile`、`.dockerignore`、`agentflow-rag/.dockerignore`（新）
- `docker-compose.yml`（改）、`.env.example`（新）、`.gitignore`（改）
- `scripts/e2e-agent.sh`、`scripts/chaos-agent.sh`（新）
- `.github/workflows/ci.yml`（新）
- `README.md`（英文，原 `README.en.md` 改写）、`README.zh.md`（原 `README.md`）
- `docs/interview-walkthrough.md`（新）、`docs/resume-bullets.md`（改）

---

### Task 0: 准备分支与基线

**Files:** 无代码改动

- [ ] **Step 1: 建分支**

```bash
cd /Users/cong/Documents/project/project4find-job/agentflow
git checkout -b feat/research-agent
```

- [ ] **Step 2: 启动本地中间件**

Mac 上的 Docker（OrbStack）可能没启动，先执行 `open -a OrbStack`，等 `docker ps` 能用后再继续。

```bash
docker compose up -d
docker start agentflow-pgvector 2>/dev/null || docker run -d --name agentflow-pgvector -p 5433:5432 \
  -e POSTGRES_PASSWORD=agentflow -e POSTGRES_DB=agentflow_rag pgvector/pgvector:pg16
```

- [ ] **Step 3: 跑基线测试并记录结果**

Run: `mvn -B -q test 2>&1 | tail -30`
Expected: 35 个测试全部 PASS。如果有失败，把失败的测试名记下来，后续任务不能新增失败，也不需要修复原本就失败的测试（在最终汇报里说明）。

---

### Task 1: common 加 tool calling 消息模型，OpenAiLlmClient 支持工具调用

**Files:**
- Create: `agentflow-common/src/main/java/com/agentflow/common/llm/ToolCall.java`
- Create: `agentflow-common/src/main/java/com/agentflow/common/llm/ToolSpec.java`
- Modify: `agentflow-common/src/main/java/com/agentflow/common/llm/ChatMessage.java`
- Modify: `agentflow-common/src/main/java/com/agentflow/common/llm/ChatRequest.java`
- Modify: `agentflow-common/src/main/java/com/agentflow/common/llm/ChatResponse.java`
- Modify: `agentflow-common/src/main/java/com/agentflow/common/trace/TraceStage.java`
- Modify: `agentflow-worker/src/main/java/com/agentflow/worker/llm/OpenAiLlmClient.java`
- Test: `agentflow-worker/src/test/java/com/agentflow/worker/llm/OpenAiLlmClientTest.java`

**Interfaces:**
- Produces:
  - `new ToolCall(String id, String name, String argumentsJson)`，带 getter/setter
  - `record ToolSpec(String name, String description, String parametersSchema)`，`parametersSchema` 是 JSON Schema 字符串
  - `ChatMessage.assistant(String content, List<ToolCall> toolCalls)`、`ChatMessage.tool(String toolCallId, String content)`，以及 `getToolCalls()`、`getToolCallId()`
  - `ChatRequest.setTools(List<ToolSpec>)` / `getTools()`
  - `ChatResponse.getToolCalls()`（永不为 null，默认空列表）/ `setToolCalls(List<ToolCall>)`
  - `TraceStage.STAGE_ADVANCED, AGENT_STEP, RESUMED, CITATION_DROPPED`

- [ ] **Step 1: 写失败的测试**

```java
package com.agentflow.worker.llm;

import com.agentflow.common.llm.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** OpenAiLlmClient:tool calling 的请求序列化与响应解析(本地 HttpServer 桩,不出网)。 */
class OpenAiLlmClientTest {

    private HttpServer server;
    private final AtomicReference<String> sentBody = new AtomicReference<>();

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/chat/completions", ex -> {
            sentBody.set(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            byte[] resp = """
                    {"choices":[{"message":{"role":"assistant","content":null,"tool_calls":[
                      {"id":"call_9","type":"function","function":{"name":"wiki_search","arguments":"{\\"query\\":\\"kafka\\"}"}}]}}],
                     "usage":{"prompt_tokens":12,"completion_tokens":7}}
                    """.getBytes(StandardCharsets.UTF_8);
            ex.getResponseHeaders().add("Content-Type", "application/json");
            ex.sendResponseHeaders(200, resp.length);
            ex.getResponseBody().write(resp);
            ex.close();
        });
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private OpenAiLlmClient client() {
        OpenAiLlmClient c = new OpenAiLlmClient();
        ReflectionTestUtils.setField(c, "baseUrl", "http://127.0.0.1:" + server.getAddress().getPort() + "/v1");
        ReflectionTestUtils.setField(c, "apiKey", "test-key");
        return c;
    }

    @Test
    void parsesToolCallsAndSerializesToolConversation() throws Exception {
        ChatRequest req = new ChatRequest("m", List.of(
                new ChatMessage("system", "s"),
                new ChatMessage("user", "q"),
                ChatMessage.assistant("thinking", List.of(
                        new ToolCall("call_1", "web_fetch", "{\"url\":\"https://a.b\"}"))),
                ChatMessage.tool("call_1", "page text")), 100);
        req.setTools(List.of(new ToolSpec("wiki_search", "search wikipedia",
                "{\"type\":\"object\",\"properties\":{\"query\":{\"type\":\"string\"}}}")));

        ChatResponse resp = client().chat(req);

        assertNull(resp.getContent());
        assertEquals(1, resp.getToolCalls().size());
        assertEquals("call_9", resp.getToolCalls().get(0).getId());
        assertEquals("wiki_search", resp.getToolCalls().get(0).getName());
        assertEquals("{\"query\":\"kafka\"}", resp.getToolCalls().get(0).getArgumentsJson());
        assertEquals(12, resp.getUsage().getPromptTokens());

        JsonNode sent = new ObjectMapper().readTree(sentBody.get());
        assertEquals("function", sent.at("/tools/0/type").asText());
        assertEquals("wiki_search", sent.at("/tools/0/function/name").asText());
        assertEquals("object", sent.at("/tools/0/function/parameters/type").asText());
        assertEquals("call_1", sent.at("/messages/2/tool_calls/0/id").asText());
        assertEquals("web_fetch", sent.at("/messages/2/tool_calls/0/function/name").asText());
        assertEquals("{\"url\":\"https://a.b\"}", sent.at("/messages/2/tool_calls/0/function/arguments").asText());
        assertEquals("tool", sent.at("/messages/3/role").asText());
        assertEquals("call_1", sent.at("/messages/3/tool_call_id").asText());
    }

    @Test
    void omitsToolsFieldWhenNoneGiven() throws Exception {
        client().chat(new ChatRequest("m", List.of(new ChatMessage("user", "hi")), 10));
        assertFalse(new ObjectMapper().readTree(sentBody.get()).has("tools"));
    }
}
```

- [ ] **Step 2: 运行测试，确认编译失败**

Run: `mvn -B -q -pl agentflow-worker -am test -Dtest=OpenAiLlmClientTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 编译失败，提示 `ToolCall` / `ToolSpec` / `ChatMessage.assistant` 不存在。

- [ ] **Step 3: 实现 common 类型**

`ToolCall.java`：

```java
package com.agentflow.common.llm;

/** 模型发起的一次工具调用(OpenAI tool_calls 的一项)。argumentsJson 是模型给的原始 JSON 字符串。 */
public class ToolCall {
    private String id;
    private String name;
    private String argumentsJson;

    public ToolCall() {}

    public ToolCall(String id, String name, String argumentsJson) {
        this.id = id;
        this.name = name;
        this.argumentsJson = argumentsJson;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getArgumentsJson() { return argumentsJson; }
    public void setArgumentsJson(String argumentsJson) { this.argumentsJson = argumentsJson; }
}
```

`ToolSpec.java`：

```java
package com.agentflow.common.llm;

/** 告诉模型"有哪些工具可用":名字、用途、参数 JSON Schema(字符串)。 */
public record ToolSpec(String name, String description, String parametersSchema) {}
```

`ChatMessage.java` 整体替换为：

```java
package com.agentflow.common.llm;

import java.util.List;

/**
 * LLM 对话消息:role = system/user/assistant/tool。
 * assistant 可带 toolCalls(模型决定调工具);tool 消息带 toolCallId(回传对应工具的结果)。
 */
public class ChatMessage {
    private String role;
    private String content;
    private List<ToolCall> toolCalls;
    private String toolCallId;

    public ChatMessage() {}

    public ChatMessage(String role, String content) {
        this.role = role;
        this.content = content;
    }

    public static ChatMessage assistant(String content, List<ToolCall> toolCalls) {
        ChatMessage m = new ChatMessage("assistant", content);
        m.toolCalls = toolCalls;
        return m;
    }

    public static ChatMessage tool(String toolCallId, String content) {
        ChatMessage m = new ChatMessage("tool", content);
        m.toolCallId = toolCallId;
        return m;
    }

    public String getRole() { return role; }
    public void setRole(String role) { this.role = role; }
    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; }
    public List<ToolCall> getToolCalls() { return toolCalls; }
    public void setToolCalls(List<ToolCall> toolCalls) { this.toolCalls = toolCalls; }
    public String getToolCallId() { return toolCallId; }
    public void setToolCallId(String toolCallId) { this.toolCallId = toolCallId; }
}
```

`ChatRequest.java`：加字段与 getter/setter，并更新类注释：

```java
/** LLM 调用请求。model 可空 → 网关用默认模型;tools 可空 → 不提供工具(纯对话)。 */
public class ChatRequest {
    private String model;
    private List<ChatMessage> messages;
    private int maxTokens = 512;
    private List<ToolSpec> tools;
    // ...现有构造器与 getter/setter 不变,追加:
    public List<ToolSpec> getTools() { return tools; }
    public void setTools(List<ToolSpec> tools) { this.tools = tools; }
}
```

`ChatResponse.java`：加字段（顶部加 `import java.util.List;`）：

```java
    /** 模型要求调用的工具;为空表示这是最终回答。永不为 null。 */
    private List<ToolCall> toolCalls = List.of();

    public List<ToolCall> getToolCalls() { return toolCalls; }
    public void setToolCalls(List<ToolCall> toolCalls) { this.toolCalls = toolCalls == null ? List.of() : toolCalls; }
```

`TraceStage.java`：

```java
public enum TraceStage {
    SUBMITTED, DECOMPOSED, DISPATCHED, WORKER_RECEIVED, PROCESSED, RESULT_SENT,
    SUBTASK_SETTLED, RETRY, DLQ, RECOVERED, TIMEOUT_REDISPATCH, TASK_FINALIZED,
    // RESEARCH_AGENT:阶段推进 / agent 每一步 / 从存档续跑 / 删除编造的引用
    STAGE_ADVANCED, AGENT_STEP, RESUMED, CITATION_DROPPED
}
```

- [ ] **Step 4: 修改 OpenAiLlmClient**

在 `OpenAiLlmClient` 中：
1. HttpClient 固定 HTTP/1.1（与 `RagClient` 一致，避免 h2c 升级问题）：

```java
    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(10)).build();
```

2. 把 `chat` 里构造 messages 的循环换成下面的代码，并在 `body` 上追加 tools（新增 import：`ToolCall`、`ToolSpec`、`java.util.ArrayList`、`java.util.List`）：

```java
            ArrayNode msgs = body.putArray("messages");
            for (ChatMessage m : request.getMessages()) {
                ObjectNode o = msgs.addObject().put("role", m.getRole());
                if (m.getContent() == null) {
                    o.putNull("content");
                } else {
                    o.put("content", m.getContent());
                }
                if (m.getToolCallId() != null) {
                    o.put("tool_call_id", m.getToolCallId());
                }
                if (m.getToolCalls() != null && !m.getToolCalls().isEmpty()) {
                    ArrayNode calls = o.putArray("tool_calls");
                    for (ToolCall c : m.getToolCalls()) {
                        ObjectNode call = calls.addObject().put("id", c.getId()).put("type", "function");
                        call.putObject("function").put("name", c.getName()).put("arguments", c.getArgumentsJson());
                    }
                }
            }
            if (request.getTools() != null && !request.getTools().isEmpty()) {
                ArrayNode tools = body.putArray("tools");
                for (ToolSpec t : request.getTools()) {
                    ObjectNode fn = tools.addObject().put("type", "function").putObject("function");
                    fn.put("name", t.name()).put("description", t.description());
                    fn.set("parameters", OM.readTree(t.parametersSchema()));
                }
            }
```

3. 把解析响应的部分换成：

```java
            JsonNode root = OM.readTree(resp.body());
            JsonNode msg = root.path("choices").path(0).path("message");
            String content = msg.path("content").isTextual() ? msg.get("content").asText() : null;
            List<ToolCall> calls = new ArrayList<>();
            for (JsonNode c : msg.path("tool_calls")) {
                calls.add(new ToolCall(c.path("id").asText(), c.path("function").path("name").asText(),
                        c.path("function").path("arguments").asText("{}")));
            }
            JsonNode u = root.path("usage");
            TokenUsage usage = new TokenUsage(
                    u.path("prompt_tokens").asInt(0), u.path("completion_tokens").asInt(0));
            ChatResponse out = new ChatResponse(content, usage, request.getModel());
            out.setToolCalls(calls);
            return out;
```

4. 类注释末尾追加一句：`支持 OpenAI 原生 tool calling(tools / tool_calls / role=tool),DeepSeek、通义千问等兼容服务同样适用。`

- [ ] **Step 5: 运行测试，确认通过**

Run: `mvn -B -q -pl agentflow-worker -am test -Dtest=OpenAiLlmClientTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS（2 个测试）。

- [ ] **Step 6: 全量回归并 commit**

Run: `mvn -B -q test 2>&1 | tail -20`，Expected：与基线一致。

```bash
git add agentflow-common agentflow-worker/src/main/java/com/agentflow/worker/llm/OpenAiLlmClient.java \
        agentflow-worker/src/test/java/com/agentflow/worker/llm/OpenAiLlmClientTest.java
git commit -m "feat(llm): OpenAI 原生 tool calling(消息模型 + 序列化/解析)"
```

---

### Task 2: agent 基础类（Prompts / TextUtil / Usage）与 Mock 剧本

**Files:**
- Create: `agentflow-worker/src/main/java/com/agentflow/worker/agent/Prompts.java`
- Create: `agentflow-worker/src/main/java/com/agentflow/worker/agent/TextUtil.java`
- Create: `agentflow-worker/src/main/java/com/agentflow/worker/agent/Usage.java`
- Modify: `agentflow-worker/src/main/java/com/agentflow/worker/llm/MockLlmClient.java`
- Test: `agentflow-worker/src/test/java/com/agentflow/worker/agent/TextUtilTest.java`
- Test: `agentflow-worker/src/test/java/com/agentflow/worker/llm/MockLlmClientTest.java`（追加用例）

**Interfaces:**
- Consumes: Task 1 中的 `ToolCall`、`ChatMessage.assistant/tool`、`ChatResponse.setToolCalls`
- Produces:
  - `Prompts.PLANNER_TAG / AGENT_TAG / SYNTHESIZER_TAG`，以及 `Prompts.PLANNER / AGENT / SYNTHESIZER / FORCE_ANSWER`
  - `TextUtil.MAX_TOOL_CHARS = 2000`、`TextUtil.parseObject(String) → JsonNode`（失败时抛 `IllegalStateException`）、`TextUtil.urls(String) → List<String>`、`TextUtil.truncate(String, int) → String`
  - `Usage`：字段 `calls, promptTokens, completionTokens, costMicro` 与 getter/setter；`static Usage of(ChatResponse, LlmModelProperties)`；`void add(Usage)`

- [ ] **Step 1: 写失败的测试**

`TextUtilTest.java`：

```java
package com.agentflow.worker.agent;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TextUtilTest {

    @Test
    void parsesJsonWrappedInMarkdownFence() {
        String s = "好的,结果如下:\n```json\n{\"answer\":\"ok\",\"sources\":[]}\n```";
        assertEquals("ok", TextUtil.parseObject(s).get("answer").asText());
    }

    @Test
    void proseWithoutJsonThrows() {
        assertThrows(IllegalStateException.class, () -> TextUtil.parseObject("Kafka is a log."));
        assertThrows(IllegalStateException.class, () -> TextUtil.parseObject(null));
    }

    @Test
    void extractsUrlsKeepingParenthesesAndDroppingTrailingPunctuation() {
        String text = "URL: https://en.wikipedia.org/wiki/Kafka_(software).\n来源: kb://ops-1, 见 http://a.b/c;";
        assertEquals(List.of("https://en.wikipedia.org/wiki/Kafka_(software)", "kb://ops-1", "http://a.b/c"),
                TextUtil.urls(text));
    }

    @Test
    void truncatesWithEllipsis() {
        assertEquals("abc…", TextUtil.truncate("abcdef", 3));
        assertEquals("ab", TextUtil.truncate("ab", 3));
        assertEquals("", TextUtil.truncate(null, 3));
    }
}
```

在 `MockLlmClientTest.java` 中追加（补 import：`com.agentflow.common.llm.*`、`com.agentflow.worker.agent.Prompts`、`com.fasterxml.jackson.databind.ObjectMapper`、`java.util.ArrayList`、`java.util.List`）：

```java
    private static final ObjectMapper OM = new ObjectMapper();

    @Test
    void plannerScriptReturnsThreeSubQuestions() throws Exception {
        ChatResponse r = new MockLlmClient().chat(new ChatRequest("mock-small", List.of(
                new ChatMessage("system", Prompts.PLANNER), new ChatMessage("user", "kafka vs rabbitmq")), 100));
        assertEquals(3, OM.readTree(r.getContent()).get("subQuestions").size());
    }

    @Test
    void agentScriptSearchesThenFetchesThenAnswers() throws Exception {
        MockLlmClient mock = new MockLlmClient();
        List<ToolSpec> tools = List.of(new ToolSpec("wiki_search", "d", "{}"));
        List<ChatMessage> msgs = new ArrayList<>(List.of(
                new ChatMessage("system", Prompts.AGENT), new ChatMessage("user", "what is kafka")));

        ChatRequest r1 = new ChatRequest("mock-small", new ArrayList<>(msgs), 100);
        r1.setTools(tools);
        ChatResponse s1 = mock.chat(r1);
        assertEquals("wiki_search", s1.getToolCalls().get(0).getName());
        assertTrue(s1.getToolCalls().get(0).getArgumentsJson().contains("what is kafka"));

        msgs.add(ChatMessage.assistant(s1.getContent(), s1.getToolCalls()));
        msgs.add(ChatMessage.tool("call_1", "标题: Apache Kafka\nURL: https://en.wikipedia.org/wiki/Apache_Kafka\n摘要: x"));
        ChatRequest r2 = new ChatRequest("mock-small", new ArrayList<>(msgs), 100);
        r2.setTools(tools);
        ChatResponse s2 = mock.chat(r2);
        assertEquals("web_fetch", s2.getToolCalls().get(0).getName());
        assertTrue(s2.getToolCalls().get(0).getArgumentsJson().contains("https://en.wikipedia.org/wiki/Apache_Kafka"));

        msgs.add(ChatMessage.assistant(s2.getContent(), s2.getToolCalls()));
        msgs.add(ChatMessage.tool("call_2", "URL: https://en.wikipedia.org/wiki/Apache_Kafka\nKafka is a log"));
        ChatRequest r3 = new ChatRequest("mock-small", new ArrayList<>(msgs), 100);
        r3.setTools(tools);
        ChatResponse s3 = mock.chat(r3);
        assertTrue(s3.getToolCalls().isEmpty());
        assertEquals("https://en.wikipedia.org/wiki/Apache_Kafka",
                OM.readTree(s3.getContent()).at("/sources/0/url").asText());
    }

    @Test
    void synthesizerScriptCitesFirstSourceWhenPresent() {
        ChatResponse r = new MockLlmClient().chat(new ChatRequest("mock-small", List.of(
                new ChatMessage("system", Prompts.SYNTHESIZER),
                new ChatMessage("user", "问题:q\n\n可引用来源:\n[1] Kafka https://x.y\n")), 100));
        assertTrue(r.getContent().contains("[1]"));
    }
```

- [ ] **Step 2: 运行测试，确认编译失败**

Run: `mvn -B -q -pl agentflow-worker -am test -Dtest='TextUtilTest,MockLlmClientTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 编译失败（`Prompts` / `TextUtil` 不存在）。

- [ ] **Step 3: 实现 Prompts / TextUtil / Usage**

`Prompts.java`：

```java
package com.agentflow.worker.agent;

/**
 * RESEARCH_AGENT 三个角色的系统提示。每个提示以角色标签开头:
 * MockLlmClient 据此识别角色、返回对应剧本(不烧钱也能把整条链路跑通)。
 */
public final class Prompts {

    public static final String PLANNER_TAG = "[planner]";
    public static final String AGENT_TAG = "[agent]";
    public static final String SYNTHESIZER_TAG = "[synthesizer]";

    public static final String PLANNER = PLANNER_TAG + " 你是研究规划助手。把用户的问题拆成 2~4 个可以独立调研、"
            + "互不重复的子问题,使用与用户问题相同的语言。只输出 JSON:{\"subQuestions\":[\"...\"]}";

    public static final String AGENT = AGENT_TAG + " 你是研究助手,负责回答一个子问题。可用工具:"
            + "wiki_search(用英文关键词搜索 Wikipedia)、web_fetch(抓取网页正文)、kb_search(检索本地知识库)。"
            + "每次调用工具前先用一句话说明思路。信息足够后停止调用工具,只输出 JSON:"
            + "{\"answer\":\"200 字以内的结论\",\"sources\":[{\"title\":\"...\",\"url\":\"...\"}]}。"
            + "sources 只能使用工具结果中出现过的 URL。使用与问题相同的语言作答。";

    public static final String FORCE_ANSWER = "已达到工具调用上限。请根据已有信息立即作答,只输出要求的 JSON。";

    public static final String SYNTHESIZER = SYNTHESIZER_TAG + " 你是研究报告撰写人。根据各子问题的结论写一篇"
            + "结构清晰的报告(400~800 字),使用与问题相同的语言。引用时在句末标注 [n],n 只能取自给定的来源列表。"
            + "只输出报告正文。";

    private Prompts() {}
}
```

`TextUtil.java`：

```java
package com.agentflow.worker.agent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** 处理模型/工具文本的小工具:宽松提取 JSON、提取 URL、截断。 */
public final class TextUtil {

    /** 单个工具结果进上下文前的截断长度(防上下文膨胀)。 */
    public static final int MAX_TOOL_CHARS = 2000;

    private static final ObjectMapper OM = new ObjectMapper();
    // 保留括号(Wikipedia 的 Kafka_(software) 这类 URL),以空白/引号/尖括号/方括号为界
    private static final Pattern URL = Pattern.compile("(?:https?|kb)://[^\\s\"'<>\\[\\]]+");

    private TextUtil() {}

    /** 从模型输出中取第一个 '{' 到最后一个 '}' 解析为 JSON:兼容 ```json 代码块和前后废话。 */
    public static JsonNode parseObject(String text) {
        if (text == null) {
            throw new IllegalStateException("LLM 输出为空");
        }
        int a = text.indexOf('{');
        int b = text.lastIndexOf('}');
        if (a < 0 || b < a) {
            throw new IllegalStateException("LLM 输出不含 JSON 对象:" + truncate(text, 200));
        }
        try {
            return OM.readTree(text.substring(a, b + 1));
        } catch (Exception e) {
            throw new IllegalStateException("LLM 输出 JSON 解析失败:" + truncate(text, 200));
        }
    }

    /** 提取文本中的 http/https/kb URL,去掉句末标点。 */
    public static List<String> urls(String text) {
        List<String> out = new ArrayList<>();
        if (text == null) {
            return out;
        }
        Matcher m = URL.matcher(text);
        while (m.find()) {
            out.add(m.group().replaceAll("[.,;:!?。，；]+$", ""));
        }
        return out;
    }

    public static String truncate(String s, int max) {
        if (s == null) {
            return "";
        }
        return s.length() <= max ? s : s.substring(0, max) + "…";
    }
}
```

`Usage.java`：

```java
package com.agentflow.worker.agent;

import com.agentflow.common.llm.ChatResponse;
import com.agentflow.common.llm.TokenUsage;
import com.agentflow.worker.llm.LlmModelProperties;
import lombok.Getter;
import lombok.Setter;

/**
 * 一次子任务处理内所有 LLM 调用的 token / 成本累计,随输出返回给页面页脚。
 * 成本以微美元(long)累加,与 TokenAccountant 同口径,避免浮点漂移。
 */
@Getter
@Setter
public class Usage {
    private int calls;
    private long promptTokens;
    private long completionTokens;
    private long costMicro;

    public static Usage of(ChatResponse resp, LlmModelProperties props) {
        Usage u = new Usage();
        if (resp == null || resp.getUsage() == null) {
            return u;
        }
        TokenUsage t = resp.getUsage();
        u.calls = 1;
        u.promptTokens = t.getPromptTokens();
        u.completionTokens = t.getCompletionTokens();
        u.costMicro = Math.round(t.total() / 1000.0
                * props.model(resp.getModel()).getPricePer1kTokens() * 1_000_000);
        return u;
    }

    public void add(Usage o) {
        if (o == null) {
            return;
        }
        calls += o.calls;
        promptTokens += o.promptTokens;
        completionTokens += o.completionTokens;
        costMicro += o.costMicro;
    }
}
```

- [ ] **Step 4: 改写 MockLlmClient**

整体替换 `MockLlmClient.java`（保留原有「摘要」行为作为兜底分支，`MockLlmClientTest` 中的原有用例必须继续通过）：

```java
package com.agentflow.worker.llm;

import com.agentflow.common.llm.ChatMessage;
import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import com.agentflow.common.llm.ToolCall;
import com.agentflow.common.llm.TokenUsage;
import com.agentflow.worker.agent.Prompts;
import com.agentflow.worker.agent.TextUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 默认 LLM 后端:确定性输出、不发网络、无需 API key——压测测调度不烧钱(master 设计文档第 149 行)。
 * 网关的限流/路由/记账对 mock 与真实模型完全一致,mock 只是把"发 HTTP"换成"本地确定性返回"。
 *
 * <p>RESEARCH_AGENT 剧本:按系统提示的角色标签({@link Prompts})分流——
 * planner 固定拆 3 个子问题;agent 第 1 步 wiki_search、第 2 步 web_fetch、第 3 步给答案;
 * synthesizer 返回引用 [1] 的报告。其余请求保持原有"摘要"行为。
 * {@code llm.mock.delay-ms} 模拟模型耗时,便于在页面上看清每一步、演示中途杀 worker。
 */
@Component
@ConditionalOnProperty(name = "llm.provider", havingValue = "mock", matchIfMissing = true)
public class MockLlmClient implements LlmClient {

    private static final ObjectMapper OM = new ObjectMapper();

    @Value("${llm.mock.delay-ms:0}")
    private long delayMs;

    @Override
    public String provider() {
        return "mock";
    }

    @Override
    public ChatResponse chat(ChatRequest request) {
        sleep();
        List<ChatMessage> msgs = request.getMessages() == null ? List.of() : request.getMessages();
        String system = msgs.stream().filter(m -> "system".equals(m.getRole()))
                .map(ChatMessage::getContent).findFirst().orElse("");
        ChatResponse resp;
        if (system.startsWith(Prompts.PLANNER_TAG)) {
            resp = plan(firstUser(msgs));
        } else if (system.startsWith(Prompts.AGENT_TAG)) {
            resp = agentStep(request, msgs);
        } else if (system.startsWith(Prompts.SYNTHESIZER_TAG)) {
            resp = synthesize(lastUser(msgs));
        } else {
            resp = summarize(lastUser(msgs));
        }
        // token 估算:chars/4(见 TokenEstimator 同款启发式);mock 无真实 usage 故本地估。
        int promptChars = msgs.stream().mapToInt(m -> m.getContent() == null ? 0 : m.getContent().length()).sum();
        int completionChars = resp.getContent() == null ? 0 : resp.getContent().length();
        resp.setUsage(new TokenUsage(Math.max(1, promptChars / 4), Math.max(1, completionChars / 4)));
        resp.setModel(request.getModel());
        return resp;
    }

    // 模拟"守规矩的模型":确定性输出符合 summarize schema 的 JSON——processor 会真的解析+校验它。
    private ChatResponse summarize(String lastUser) {
        String trimmed = lastUser.length() > 120 ? lastUser.substring(0, 120) : lastUser;
        return new ChatResponse(OM.createObjectNode().put("summary", "SUMMARY: " + trimmed.trim()).toString(),
                null, null);
    }

    private ChatResponse plan(String question) {
        ObjectNode out = OM.createObjectNode();
        out.putArray("subQuestions")
                .add(question + " — 背景与定义")
                .add(question + " — 关键取舍")
                .add(question + " — 实际案例");
        return new ChatResponse(out.toString(), null, null);
    }

    private ChatResponse agentStep(ChatRequest req, List<ChatMessage> msgs) {
        String question = firstUser(msgs);
        List<String> toolOutputs = msgs.stream().filter(m -> "tool".equals(m.getRole()))
                .map(ChatMessage::getContent).toList();
        boolean toolsOffered = req.getTools() != null && !req.getTools().isEmpty();
        if (toolsOffered && toolOutputs.isEmpty()) {
            return withCall("MOCK 思路:先在 Wikipedia 搜索关键词。",
                    new ToolCall("call_1", "wiki_search", OM.createObjectNode().put("query", question).toString()));
        }
        if (toolsOffered && toolOutputs.size() == 1) {
            String url = TextUtil.urls(toolOutputs.get(0)).stream()
                    .filter(u -> u.startsWith("http")).findFirst().orElse(null);
            if (url != null) {
                return withCall("MOCK 思路:打开第一条结果阅读正文。",
                        new ToolCall("call_2", "web_fetch", OM.createObjectNode().put("url", url).toString()));
            }
        }
        List<String> urls = toolOutputs.stream().flatMap(o -> TextUtil.urls(o).stream())
                .distinct().limit(2).toList();
        ObjectNode ans = OM.createObjectNode().put("answer",
                "MOCK 结论:关于「" + question + "」,共查阅 " + toolOutputs.size() + " 份工具结果。");
        ArrayNode src = ans.putArray("sources");
        for (int i = 0; i < urls.size(); i++) {
            src.addObject().put("title", "Source " + (i + 1)).put("url", urls.get(i));
        }
        return new ChatResponse(ans.toString(), null, null);
    }

    private ChatResponse synthesize(String user) {
        boolean hasSource = user.contains("\n[1] ");
        return new ChatResponse("MOCK 报告:以下综合了各子问题的结论" + (hasSource ? " [1]" : "") + "。\n\n"
                + "(mock 模式不调用真实模型;配置 LLM_PROVIDER=openai 后这里是真实报告。)", null, null);
    }

    private static ChatResponse withCall(String thought, ToolCall call) {
        ChatResponse r = new ChatResponse(thought, null, null);
        r.setToolCalls(List.of(call));
        return r;
    }

    private static String firstUser(List<ChatMessage> msgs) {
        return msgs.stream().filter(m -> "user".equals(m.getRole())).map(ChatMessage::getContent)
                .findFirst().orElse("");
    }

    private static String lastUser(List<ChatMessage> msgs) {
        String last = "";
        for (ChatMessage m : msgs) {
            if ("user".equals(m.getRole()) && m.getContent() != null) {
                last = m.getContent();
            }
        }
        return last;
    }

    private void sleep() {
        if (delayMs <= 0) {
            return;
        }
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
```

- [ ] **Step 5: 运行测试，确认通过**

Run: `mvn -B -q -pl agentflow-worker -am test -Dtest='TextUtilTest,MockLlmClientTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 全部 PASS，包括原有用例。

- [ ] **Step 6: 全量 worker 回归并 commit**

Run: `mvn -B -q -pl agentflow-worker -am test 2>&1 | tail -20`，Expected：与基线一致。

```bash
git add agentflow-worker
git commit -m "feat(agent): Prompts/TextUtil/Usage + MockLlmClient 研究 agent 剧本"
```

---

### Task 3: 工具（AgentTool / UrlGuard / web_fetch / wiki_search / kb_search）

**Files:**
- Create: `agentflow-worker/src/main/java/com/agentflow/worker/agent/tool/AgentTool.java`
- Create: `agentflow-worker/src/main/java/com/agentflow/worker/agent/tool/UrlGuard.java`
- Create: `agentflow-worker/src/main/java/com/agentflow/worker/agent/tool/WebFetchTool.java`
- Create: `agentflow-worker/src/main/java/com/agentflow/worker/agent/tool/WikiSearchTool.java`
- Create: `agentflow-worker/src/main/java/com/agentflow/worker/agent/tool/KbSearchTool.java`
- Test: `agentflow-worker/src/test/java/com/agentflow/worker/agent/tool/WebFetchToolTest.java`
- Test: `agentflow-worker/src/test/java/com/agentflow/worker/agent/tool/WikiSearchToolTest.java`

**Interfaces:**
- Consumes: `RagClient.search(String, int) → List<Hit>`、`RagProperties.getTopK()`、`Hit.id()/text()`
- Produces:
  - `interface AgentTool { String name(); String description(); String parametersSchema(); String run(JsonNode args) throws Exception; }`
  - `UrlGuard.check(URI)`：被拒时抛 `IllegalArgumentException`，消息包含「内网」「只允许」或「无法解析」
  - 三个工具 bean 的 name 分别为 `web_fetch`、`wiki_search`、`kb_search`。每个工具的输出中，来源都以 `URL: <url>` 或 `来源: kb://<id>` 的形式出现，供 `TextUtil.urls` 提取

- [ ] **Step 1: 写失败的测试**

`WebFetchToolTest.java`：

```java
package com.agentflow.worker.agent.tool;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class WebFetchToolTest {

    private static final ObjectMapper OM = new ObjectMapper();
    private HttpServer server;
    private String base;

    /** 测试专用:放行本地桩(127.0.0.1),其余地址仍走真实 SSRF 校验。 */
    private final UrlGuard allowLocalStub = new UrlGuard() {
        @Override
        public void check(URI uri) {
            if (!"127.0.0.1".equals(uri.getHost())) {
                super.check(uri);
            }
        }
    };

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/page", ex -> {
            byte[] b = "<html><head><style>p{}</style><script>alert(1)</script></head><body><p>Kafka&amp;logs</p></body></html>"
                    .getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, b.length);
            ex.getResponseBody().write(b);
            ex.close();
        });
        server.createContext("/redirect", ex -> {
            ex.getResponseHeaders().add("Location", "http://169.254.169.254/latest/meta-data");
            ex.sendResponseHeaders(302, -1);
            ex.close();
        });
        server.start();
        base = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private String fetch(UrlGuard guard, String url) throws Exception {
        return new WebFetchTool(guard).run(OM.createObjectNode().put("url", url));
    }

    @Test
    void returnsVisibleTextWithUrlHeader() throws Exception {
        String out = fetch(allowLocalStub, base + "/page");
        assertTrue(out.startsWith("URL: " + base + "/page\n"));
        assertTrue(out.contains("Kafka&logs"));
        assertFalse(out.contains("alert"));
        assertFalse(out.contains("<p>"));
    }

    @Test
    void rejectsRedirectIntoPrivateNetwork() {
        Exception e = assertThrows(IllegalArgumentException.class, () -> fetch(allowLocalStub, base + "/redirect"));
        assertTrue(e.getMessage().contains("内网"));
    }

    @Test
    void realGuardRejectsLoopbackPrivateLinkLocalCgnatAndNonHttp() {
        UrlGuard guard = new UrlGuard();
        for (String url : new String[]{
                "http://127.0.0.1/", "http://localhost:3306/", "http://10.0.0.5/", "http://172.20.0.3:6379/",
                "http://192.168.1.1/", "http://169.254.169.254/", "http://100.65.10.56:8080/",
                "http://[::1]/", "http://0.0.0.0/", "file:///etc/passwd", "ftp://example.com/"}) {
            assertThrows(IllegalArgumentException.class, () -> guard.check(URI.create(url)), url);
        }
    }

    @Test
    void htmlToTextStripsTagsScriptsAndEntities() {
        assertEquals("a b & c", WebFetchTool.htmlToText("<div>a</div><script>x()</script><b>b</b> &amp; c"));
    }
}
```

`WikiSearchToolTest.java`：

```java
package com.agentflow.worker.agent.tool;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** WikiSearchTool:用本地桩模拟 Wikipedia 的 search + summary 两个接口。 */
class WikiSearchToolTest {

    private HttpServer server;
    private final AtomicReference<String> userAgent = new AtomicReference<>();

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/w/api.php", ex -> {
            userAgent.set(ex.getRequestHeaders().getFirst("User-Agent"));
            respond(ex, "{\"query\":{\"search\":[{\"title\":\"Apache Kafka\"}]}}");
        });
        server.createContext("/api/rest_v1/page/summary/", ex -> respond(ex,
                "{\"extract\":\"Kafka is a distributed event store.\","
                        + "\"content_urls\":{\"desktop\":{\"page\":\"https://en.wikipedia.org/wiki/Apache_Kafka\"}}}"));
        server.start();
    }

    private static void respond(com.sun.net.httpserver.HttpExchange ex, String body) throws java.io.IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(200, b.length);
        ex.getResponseBody().write(b);
        ex.close();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    @Test
    void formatsTitleUrlAndSummary() throws Exception {
        WikiSearchTool tool = new WikiSearchTool("http://127.0.0.1:" + server.getAddress().getPort());
        String out = tool.run(new ObjectMapper().createObjectNode().put("query", "kafka"));
        assertTrue(out.contains("标题: Apache Kafka"));
        assertTrue(out.contains("URL: https://en.wikipedia.org/wiki/Apache_Kafka"));
        assertTrue(out.contains("Kafka is a distributed event store."));
        assertTrue(userAgent.get().startsWith("AgentFlow/"));
    }

    @Test
    void blankQueryRejected() {
        WikiSearchTool tool = new WikiSearchTool("http://127.0.0.1:1");
        assertThrows(IllegalArgumentException.class,
                () -> tool.run(new ObjectMapper().createObjectNode().put("query", " ")));
    }
}
```

- [ ] **Step 2: 运行测试，确认编译失败**

Run: `mvn -B -q -pl agentflow-worker -am test -Dtest='WebFetchToolTest,WikiSearchToolTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 编译失败。

- [ ] **Step 3: 实现**

`AgentTool.java`：

```java
package com.agentflow.worker.agent.tool;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * agent 可调用的工具。实现类加 {@code @Component} 即被 AgentLoop 自动收录(与 SkillRegistry 同一套
 * Spring 收集 List 的做法,新增工具零配置)。约定:工具只读、无副作用——这是"崩溃后重做某一步无害"的前提。
 */
public interface AgentTool {

    String name();

    /** 给模型看的用途说明。 */
    String description();

    /** 参数 JSON Schema(字符串)。 */
    String parametersSchema();

    /** 返回给模型看的文本。抛出的异常由 AgentLoop 转成 "ERROR: ..." 回给模型,不中断循环。 */
    String run(JsonNode args) throws Exception;
}
```

`UrlGuard.java`：

```java
package com.agentflow.worker.agent.tool;

import org.springframework.stereotype.Component;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;

/**
 * SSRF 防护:web_fetch 的 URL 由模型给出(甚至可能来自网页里的注入指令),不能让它访问
 * docker 内网的 MySQL/Redis、云厂商元数据地址或 tailnet。只放行 http/https + 公网地址。
 * ponytail: 校验与真正连接之间各解析一次 DNS(存在 DNS rebinding 窗口);需要时改为解析一次后按 IP 直连。
 */
@Component
public class UrlGuard {

    public void check(URI uri) {
        String scheme = uri.getScheme();
        if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
            throw new IllegalArgumentException("只允许 http/https:" + uri);
        }
        String host = uri.getHost();
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("URL 缺少主机名:" + uri);
        }
        InetAddress[] addrs;
        try {
            addrs = InetAddress.getAllByName(host);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("无法解析主机:" + host);
        }
        for (InetAddress a : addrs) {
            if (isBlocked(a)) {
                throw new IllegalArgumentException("拒绝访问内网地址:" + host + " → " + a.getHostAddress());
            }
        }
    }

    static boolean isBlocked(InetAddress a) {
        byte[] b = a.getAddress();
        boolean cgnat = b.length == 4 && (b[0] & 0xff) == 100 && (b[1] & 0xc0) == 64;   // 100.64/10(Tailscale 等)
        boolean ula = a instanceof Inet6Address && (b[0] & 0xfe) == 0xfc;                // fc00::/7
        return a.isLoopbackAddress() || a.isSiteLocalAddress() || a.isLinkLocalAddress()
                || a.isAnyLocalAddress() || a.isMulticastAddress() || cgnat || ula;
    }
}
```

`WebFetchTool.java`：

```java
package com.agentflow.worker.agent.tool;

import com.agentflow.worker.agent.TextUtil;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * 抓取网页正文。手动跟随重定向,每一跳都过 {@link UrlGuard}(防"公网地址 302 到内网")。
 * 只读前 1MB、10s 超时、正文截断到 2000 字。
 * ponytail: 按 UTF-8 解码,不识别其他 charset;遇到乱码页再按 Content-Type 解码。
 */
@Component
public class WebFetchTool implements AgentTool {

    static final int MAX_BYTES = 1_000_000;
    static final int MAX_REDIRECTS = 3;

    private final UrlGuard guard;
    private final HttpClient http = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NEVER)
            .connectTimeout(Duration.ofSeconds(5)).build();

    public WebFetchTool(UrlGuard guard) {
        this.guard = guard;
    }

    @Override
    public String name() {
        return "web_fetch";
    }

    @Override
    public String description() {
        return "抓取一个公开网页,返回正文前 2000 字。url 必须是 http/https 公网地址。";
    }

    @Override
    public String parametersSchema() {
        return "{\"type\":\"object\",\"properties\":{\"url\":{\"type\":\"string\",\"description\":\"要抓取的网页 URL\"}},"
                + "\"required\":[\"url\"]}";
    }

    @Override
    public String run(JsonNode args) throws Exception {
        URI uri = URI.create(args.path("url").asText("").trim());
        for (int hop = 0; hop <= MAX_REDIRECTS; hop++) {
            guard.check(uri);
            HttpRequest req = HttpRequest.newBuilder(uri).GET()
                    .header("User-Agent", WikiSearchTool.USER_AGENT)
                    .timeout(Duration.ofSeconds(10)).build();
            HttpResponse<InputStream> resp = http.send(req, HttpResponse.BodyHandlers.ofInputStream());
            int sc = resp.statusCode();
            if (sc / 100 == 3) {
                resp.body().close();
                String loc = resp.headers().firstValue("location")
                        .orElseThrow(() -> new IllegalStateException("重定向缺少 Location"));
                uri = uri.resolve(loc);
                continue;
            }
            byte[] bytes;
            try (InputStream in = resp.body()) {
                bytes = in.readNBytes(MAX_BYTES);
            }
            if (sc / 100 != 2) {
                throw new IllegalStateException("HTTP " + sc);
            }
            String text = htmlToText(new String(bytes, StandardCharsets.UTF_8));
            return "URL: " + uri + "\n" + TextUtil.truncate(text, TextUtil.MAX_TOOL_CHARS);
        }
        throw new IllegalStateException("重定向超过 " + MAX_REDIRECTS + " 次");
    }

    static String htmlToText(String html) {
        return html.replaceAll("(?is)<(script|style|noscript)[^>]*>.*?</\\1>", " ")
                .replaceAll("(?s)<[^>]+>", " ")
                .replace("&nbsp;", " ").replace("&lt;", "<").replace("&gt;", ">")
                .replace("&quot;", "\"").replace("&#39;", "'").replace("&amp;", "&")
                .replaceAll("\\s+", " ").trim();
    }
}
```

`WikiSearchTool.java`：

```java
package com.agentflow.worker.agent.tool;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/** 搜索英文 Wikipedia(免费、无需 key):search 接口取前 3 个词条,再逐个取 summary。 */
@Component
public class WikiSearchTool implements AgentTool {

    /** Wikimedia API 政策要求可识别的 User-Agent。 */
    static final String USER_AGENT = "AgentFlow/0.1 (portfolio research-agent demo)";
    private static final ObjectMapper OM = new ObjectMapper();

    private final String baseUrl;
    private final HttpClient http = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .connectTimeout(Duration.ofSeconds(5)).build();

    public WikiSearchTool(@Value("${agent.wiki.base-url:https://en.wikipedia.org}") String baseUrl) {
        this.baseUrl = baseUrl;
    }

    @Override
    public String name() {
        return "wiki_search";
    }

    @Override
    public String description() {
        return "搜索英文 Wikipedia,返回最相关的 3 个词条(标题、URL、摘要)。query 请用英文关键词。";
    }

    @Override
    public String parametersSchema() {
        return "{\"type\":\"object\",\"properties\":{\"query\":{\"type\":\"string\",\"description\":\"英文搜索关键词\"}},"
                + "\"required\":[\"query\"]}";
    }

    @Override
    public String run(JsonNode args) throws Exception {
        String q = args.path("query").asText("").trim();
        if (q.isEmpty()) {
            throw new IllegalArgumentException("query 不能为空");
        }
        JsonNode search = get(baseUrl + "/w/api.php?action=query&list=search&format=json&srlimit=3&srsearch=" + enc(q));
        StringBuilder sb = new StringBuilder();
        for (JsonNode hit : search.path("query").path("search")) {
            String title = hit.path("title").asText();
            String slug = enc(title.replace(' ', '_'));
            JsonNode s = get(baseUrl + "/api/rest_v1/page/summary/" + slug);
            sb.append("标题: ").append(title).append('\n')
                    .append("URL: ").append(s.path("content_urls").path("desktop").path("page")
                            .asText("https://en.wikipedia.org/wiki/" + slug)).append('\n')
                    .append("摘要: ").append(s.path("extract").asText("")).append("\n\n");
        }
        return sb.isEmpty() ? "没有找到相关词条:" + q : sb.toString().trim();
    }

    private JsonNode get(String url) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url)).GET()
                .header("User-Agent", USER_AGENT)
                .timeout(Duration.ofSeconds(10)).build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() / 100 != 2) {
            throw new IllegalStateException("Wikipedia HTTP " + resp.statusCode());
        }
        return OM.readTree(resp.body());
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
```

`KbSearchTool.java`：

```java
package com.agentflow.worker.agent.tool;

import com.agentflow.worker.rag.Hit;
import com.agentflow.worker.rag.RagClient;
import com.agentflow.worker.rag.RagProperties;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

/** 检索本地知识库(复用现有 RAG 服务)。来源以 kb://<docId> 标注,供引用校验使用。 */
@Component
@RequiredArgsConstructor
public class KbSearchTool implements AgentTool {

    private final RagClient ragClient;
    private final RagProperties props;

    @Override
    public String name() {
        return "kb_search";
    }

    @Override
    public String description() {
        return "检索本地知识库(项目文档、运维笔记),返回最相关的片段及来源。";
    }

    @Override
    public String parametersSchema() {
        return "{\"type\":\"object\",\"properties\":{\"query\":{\"type\":\"string\",\"description\":\"检索内容\"}},"
                + "\"required\":[\"query\"]}";
    }

    @Override
    public String run(JsonNode args) {
        String q = args.path("query").asText("").trim();
        if (q.isEmpty()) {
            throw new IllegalArgumentException("query 不能为空");
        }
        List<Hit> hits = ragClient.search(q, props.getTopK());
        if (hits.isEmpty()) {
            return "知识库没有相关内容:" + q;
        }
        StringBuilder sb = new StringBuilder();
        for (Hit h : hits) {
            sb.append("来源: kb://").append(h.id()).append('\n').append(h.text()).append("\n\n");
        }
        return sb.toString().trim();
    }
}
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `mvn -B -q -pl agentflow-worker -am test -Dtest='WebFetchToolTest,WikiSearchToolTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 全部 PASS。

- [ ] **Step 5: commit**

```bash
git add agentflow-worker
git commit -m "feat(agent): 三个只读工具 wiki_search/web_fetch/kb_search + SSRF 防护"
```

---

### Task 4: AgentCheckpoint（Redis 存档）

**Files:**
- Create: `agentflow-worker/src/main/java/com/agentflow/worker/agent/AgentStep.java`
- Create: `agentflow-worker/src/main/java/com/agentflow/worker/agent/AgentCheckpoint.java`
- Test: `agentflow-worker/src/test/java/com/agentflow/worker/agent/AgentCheckpointTest.java`

**Interfaces:**
- Consumes: `ChatMessage`、`ToolCall`（Task 1）、`Usage`（Task 2）
- Produces:
  - `record AgentStep(int step, ChatMessage assistant, List<ChatMessage> toolResults, Usage usage)`
  - `AgentCheckpoint.load(String subtaskUuid) → List<AgentStep>`：只返回从第 1 步开始连续的步骤；遇到重复步号会跳过；存档损坏时清空并返回空列表
  - `AgentCheckpoint.append(String subtaskUuid, AgentStep step)`

- [ ] **Step 1: 写失败的测试**（需要本地 Redis 在 6379 运行）

```java
package com.agentflow.worker.agent;

import com.agentflow.common.llm.ChatMessage;
import com.agentflow.common.llm.ToolCall;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** AgentCheckpoint 集成测试(真 Redis,localhost:6379)。 */
class AgentCheckpointTest {

    private static RedissonClient redisson;
    private static AgentCheckpoint checkpoint;

    @BeforeAll
    static void connect() {
        Config c = new Config();
        c.useSingleServer().setAddress("redis://localhost:6379");
        redisson = Redisson.create(c);
        checkpoint = new AgentCheckpoint(redisson);
    }

    @AfterAll
    static void close() {
        redisson.shutdown();
    }

    private static AgentStep step(int n) {
        ChatMessage assistant = ChatMessage.assistant("think " + n,
                List.of(new ToolCall("call_" + n, "wiki_search", "{\"query\":\"q" + n + "\"}")));
        Usage u = new Usage();
        u.setCalls(1);
        u.setPromptTokens(10);
        return new AgentStep(n, assistant, List.of(ChatMessage.tool("call_" + n, "URL: https://x/" + n)), u);
    }

    @Test
    void appendThenLoadRoundTripsWithTtl() {
        String id = UUID.randomUUID().toString();
        checkpoint.append(id, step(1));
        checkpoint.append(id, step(2));

        List<AgentStep> steps = checkpoint.load(id);
        assertEquals(2, steps.size());
        assertEquals("wiki_search", steps.get(1).assistant().getToolCalls().get(0).getName());
        assertEquals("call_2", steps.get(1).toolResults().get(0).getToolCallId());
        assertEquals(10, steps.get(0).usage().getPromptTokens());
        assertTrue(redisson.getList(AgentCheckpoint.PREFIX + id, StringCodec.INSTANCE).remainTimeToLive() > 0);
    }

    @Test
    void duplicateStepNumbersAreSkipped() {
        String id = UUID.randomUUID().toString();
        checkpoint.append(id, step(1));
        checkpoint.append(id, step(1));   // 并发双跑时可能出现的重复写入
        checkpoint.append(id, step(2));
        assertEquals(List.of(1, 2), checkpoint.load(id).stream().map(AgentStep::step).toList());
    }

    @Test
    void corruptedCheckpointIsDroppedAndStartsFresh() {
        String id = UUID.randomUUID().toString();
        redisson.getList(AgentCheckpoint.PREFIX + id, StringCodec.INSTANCE).add("{not json");
        assertTrue(checkpoint.load(id).isEmpty());
        assertFalse(redisson.getList(AgentCheckpoint.PREFIX + id, StringCodec.INSTANCE).isExists());
    }
}
```

- [ ] **Step 2: 运行测试，确认编译失败**

Run: `mvn -B -q -pl agentflow-worker -am test -Dtest=AgentCheckpointTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 编译失败。

- [ ] **Step 3: 实现**

`AgentStep.java`：

```java
package com.agentflow.worker.agent;

import com.agentflow.common.llm.ChatMessage;

import java.util.List;

/** 一步完整的 ReAct 存档:模型的决策(含 toolCalls)+ 每个工具的结果 + 本步 token 用量。 */
public record AgentStep(int step, ChatMessage assistant, List<ChatMessage> toolResults, Usage usage) {}
```

`AgentCheckpoint.java`：

```java
package com.agentflow.worker.agent;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RList;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * agent 续跑存档(Redis list,key = agent:ckpt:{subtaskUuid},每步一条 JSON,TTL 24h)。
 * 它只是"续跑缓存",不是业务状态:子任务成败仍只由 server 在 MySQL 里 CAS 落定,worker 不连 MySQL。
 * 丢了最近一步的存档 = 该步重做一次;工具只读,重做无副作用。
 * ponytail: 超时重投与原 worker 同时存活时可能双跑,load 按步号去重兜底;需要严格互斥再加 Redis 锁。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AgentCheckpoint {

    static final String PREFIX = "agent:ckpt:";
    static final Duration TTL = Duration.ofHours(24);
    private static final ObjectMapper OM = new ObjectMapper();

    private final RedissonClient redisson;

    /** 返回从第 1 步开始连续的已完成步骤;存档损坏则清空、从头开始。 */
    public List<AgentStep> load(String subtaskUuid) {
        RList<String> list = list(subtaskUuid);
        List<AgentStep> steps = new ArrayList<>();
        try {
            for (String json : list.readAll()) {
                AgentStep s = OM.readValue(json, AgentStep.class);
                if (s.step() == steps.size() + 1) {
                    steps.add(s);
                }
            }
        } catch (Exception e) {
            log.warn("agent 存档损坏,丢弃并从头开始 subtaskUuid={}", subtaskUuid, e);
            list.delete();
            return List.of();
        }
        return steps;
    }

    public void append(String subtaskUuid, AgentStep step) {
        RList<String> list = list(subtaskUuid);
        try {
            list.add(OM.writeValueAsString(step));
        } catch (Exception e) {
            throw new IllegalStateException("agent 存档序列化失败", e);
        }
        list.expire(TTL);
    }

    private RList<String> list(String subtaskUuid) {
        return redisson.getList(PREFIX + subtaskUuid, StringCodec.INSTANCE);
    }
}
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `mvn -B -q -pl agentflow-worker -am test -Dtest=AgentCheckpointTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 3 个测试 PASS。

- [ ] **Step 5: commit**

```bash
git add agentflow-worker
git commit -m "feat(agent): Redis 续跑存档 AgentCheckpoint"
```

---

### Task 5: AgentLoop（ReAct 循环 + 续跑 + 去重 + 防编造引用）

**Files:**
- Create: `agentflow-worker/src/main/java/com/agentflow/worker/agent/AgentRun.java`
- Create: `agentflow-worker/src/main/java/com/agentflow/worker/agent/AgentLoop.java`
- Test: `agentflow-worker/src/test/java/com/agentflow/worker/agent/AgentLoopTest.java`

**Interfaces:**
- Consumes: `LlmGateway.chat(ChatRequest)`、`AgentTool`、`AgentCheckpoint.load/append`、`AgentStep`、`TraceEmitter.emit(String traceId, Long taskId, Long subtaskId, TraceStage stage, String status, String detail)`、`LlmModelProperties`、`Usage`、`TextUtil`、`Prompts`
- Produces:
  - `record AgentRun(Long taskId, Long subtaskId, String subtaskUuid, String question, String model)`
  - `AgentLoop.run(AgentRun) → ObjectNode`，内容为 `{question, answer, sources:[{title,url}], usage:{calls,promptTokens,completionTokens,costMicro}, steps}`
  - trace 的 detail 格式（Task 8 的页面依赖这些字段）：
    - `AGENT_STEP` 中间步：`{step, thought, workerId, calls:[{tool,args,resultPreview}]}`
    - `AGENT_STEP` 最终步：`{step, final:true, answer, workerId}`
    - `RESUMED`：`{fromStep, workerId}`（`fromStep` = 已完成的步数）
    - `CITATION_DROPPED`：`{urls:[...]}`

- [ ] **Step 1: 写失败的测试**

```java
package com.agentflow.worker.agent;

import com.agentflow.common.llm.*;
import com.agentflow.common.trace.TraceStage;
import com.agentflow.worker.agent.tool.AgentTool;
import com.agentflow.worker.llm.LlmGateway;
import com.agentflow.worker.llm.LlmModelProperties;
import com.agentflow.worker.trace.TraceEmitter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class AgentLoopTest {

    /** 可计数的假工具:返回固定文本或抛出指定异常。 */
    static class FakeTool implements AgentTool {
        int calls;
        String output = "标题: Kafka\nURL: https://en.wikipedia.org/wiki/Apache_Kafka\n摘要: log";
        RuntimeException fail;

        public String name() { return "search"; }
        public String description() { return "d"; }
        public String parametersSchema() { return "{\"type\":\"object\"}"; }
        public String run(JsonNode args) {
            calls++;
            if (fail != null) throw fail;
            return output;
        }
    }

    private final LlmGateway gateway = mock(LlmGateway.class);
    private final AgentCheckpoint checkpoint = mock(AgentCheckpoint.class);
    private final TraceEmitter trace = mock(TraceEmitter.class);
    private final FakeTool tool = new FakeTool();
    private final AgentRun run = new AgentRun(1L, 2L, "uuid-1", "what is kafka", "m");

    private AgentLoop loop() {
        LlmModelProperties props = new LlmModelProperties();
        LlmModelProperties.Model m = new LlmModelProperties.Model();
        m.setName("m");
        m.setPricePer1kTokens(1.0);
        props.setModels(List.of(m));
        when(checkpoint.load(anyString())).thenReturn(List.of());
        return new AgentLoop(gateway, List.of(tool), checkpoint, trace, props);
    }

    private static ChatResponse call(String id, String args) {
        ChatResponse r = new ChatResponse("思路 " + id, new TokenUsage(100, 10), "m");
        r.setToolCalls(List.of(new ToolCall(id, "search", args)));
        return r;
    }

    private static ChatResponse answer(String content) {
        return new ChatResponse(content, new TokenUsage(100, 10), "m");
    }

    private List<ChatRequest> requests(int n) {
        ArgumentCaptor<ChatRequest> c = ArgumentCaptor.forClass(ChatRequest.class);
        verify(gateway, times(n)).chat(c.capture());
        return c.getAllValues();
    }

    private List<String> detailsOf(TraceStage stage) {
        ArgumentCaptor<String> d = ArgumentCaptor.forClass(String.class);
        verify(trace, atLeast(0)).emit(anyString(), anyLong(), anyLong(), eq(stage), any(), d.capture());
        return d.getAllValues();
    }

    @Test
    void searchesThenAnswersWithVerifiedSource() {
        AgentLoop loop = loop();
        when(gateway.chat(any())).thenReturn(
                call("c1", "{\"q\":\"kafka\"}"),
                answer("{\"answer\":\"Kafka 是分布式日志\",\"sources\":[{\"title\":\"Kafka\","
                        + "\"url\":\"https://en.wikipedia.org/wiki/Apache_Kafka\"}]}"));

        ObjectNode out = loop.run(run);

        assertEquals("Kafka 是分布式日志", out.get("answer").asText());
        assertEquals("https://en.wikipedia.org/wiki/Apache_Kafka", out.at("/sources/0/url").asText());
        assertEquals(2, out.at("/usage/calls").asInt());
        assertEquals(2, out.get("steps").asInt());
        assertEquals(1, tool.calls);
        verify(checkpoint, times(1)).append(eq("uuid-1"), argThat(s -> s.step() == 1));
        List<ChatRequest> reqs = requests(2);
        assertEquals("search", reqs.get(0).getTools().get(0).name());
        assertEquals("tool", reqs.get(1).getMessages().get(3).getRole());
        assertEquals("c1", reqs.get(1).getMessages().get(3).getToolCallId());
        assertTrue(detailsOf(TraceStage.AGENT_STEP).get(0).contains("\"tool\":\"search\""));
    }

    @Test
    void fabricatedCitationIsDroppedAndTraced() {
        AgentLoop loop = loop();
        when(gateway.chat(any())).thenReturn(call("c1", "{}"),
                answer("{\"answer\":\"a\",\"sources\":[{\"title\":\"x\",\"url\":\"https://made.up/page\"}]}"));

        ObjectNode out = loop.run(run);

        assertEquals(0, out.get("sources").size());
        assertTrue(detailsOf(TraceStage.CITATION_DROPPED).get(0).contains("https://made.up/page"));
    }

    @Test
    void toolErrorIsReturnedToModelInsteadOfThrowing() {
        AgentLoop loop = loop();
        tool.fail = new IllegalStateException("boom");
        when(gateway.chat(any())).thenReturn(call("c1", "{}"), answer("{\"answer\":\"a\",\"sources\":[]}"));

        loop.run(run);

        String toolMsg = requests(2).get(1).getMessages().get(3).getContent();
        assertTrue(toolMsg.startsWith("ERROR: "));
        assertTrue(toolMsg.contains("boom"));
    }

    @Test
    void malformedArgumentsReturnedAsError() {
        AgentLoop loop = loop();
        when(gateway.chat(any())).thenReturn(call("c1", "{not json"), answer("{\"answer\":\"a\",\"sources\":[]}"));

        loop.run(run);

        assertEquals(0, tool.calls);
        assertTrue(requests(2).get(1).getMessages().get(3).getContent().startsWith("ERROR: "));
    }

    @Test
    void unknownToolReturnedAsError() {
        AgentLoop loop = loop();
        ChatResponse bad = new ChatResponse("t", new TokenUsage(1, 1), "m");
        bad.setToolCalls(List.of(new ToolCall("c1", "rm_rf", "{}")));
        when(gateway.chat(any())).thenReturn(bad, answer("{\"answer\":\"a\",\"sources\":[]}"));

        loop.run(run);

        assertTrue(requests(2).get(1).getMessages().get(3).getContent().contains("未知工具 rm_rf"));
    }

    @Test
    void repeatedIdenticalCallIsServedFromCache() {
        AgentLoop loop = loop();
        when(gateway.chat(any())).thenReturn(call("c1", "{\"q\":\"k\"}"), call("c2", "{\"q\":\"k\"}"),
                answer("{\"answer\":\"a\",\"sources\":[]}"));

        loop.run(run);

        assertEquals(1, tool.calls);
    }

    @Test
    void stepLimitForcesAnswerWithoutTools() {
        AgentLoop loop = loop();
        List<ChatResponse> script = new ArrayList<>();
        for (int i = 1; i <= AgentLoop.MAX_STEPS; i++) {
            script.add(call("c" + i, "{\"q\":\"" + i + "\"}"));
        }
        script.add(answer("{\"answer\":\"forced\",\"sources\":[]}"));
        when(gateway.chat(any())).thenReturn(script.get(0), script.subList(1, script.size()).toArray(new ChatResponse[0]));

        ObjectNode out = loop.run(run);

        assertEquals("forced", out.get("answer").asText());
        List<ChatRequest> reqs = requests(AgentLoop.MAX_STEPS + 1);
        assertNull(reqs.get(AgentLoop.MAX_STEPS).getTools());
        assertEquals(Prompts.FORCE_ANSWER, reqs.get(AgentLoop.MAX_STEPS).getMessages()
                .get(reqs.get(AgentLoop.MAX_STEPS).getMessages().size() - 1).getContent());
    }

    @Test
    void resumesFromCheckpointWithoutRerunningTools() {
        AgentLoop loop = loop();
        List<AgentStep> saved = new ArrayList<>();
        for (int n = 1; n <= 2; n++) {
            ChatMessage a = ChatMessage.assistant("t" + n, List.of(new ToolCall("c" + n, "search", "{\"q\":\"" + n + "\"}")));
            saved.add(new AgentStep(n, a, List.of(ChatMessage.tool("c" + n, "URL: https://ex.com/" + n)), new Usage()));
        }
        when(checkpoint.load("uuid-1")).thenReturn(saved);
        when(gateway.chat(any())).thenReturn(
                answer("{\"answer\":\"done\",\"sources\":[{\"title\":\"e\",\"url\":\"https://ex.com/2\"}]}"));

        ObjectNode out = loop.run(run);

        assertEquals(0, tool.calls);
        assertEquals(6, requests(1).get(0).getMessages().size());   // system + user + 2×(assistant + tool)
        assertEquals("https://ex.com/2", out.at("/sources/0/url").asText());
        assertEquals(3, out.get("steps").asInt());
        assertTrue(detailsOf(TraceStage.RESUMED).get(0).contains("\"fromStep\":2"));
    }

    @Test
    void finalAnswerInProseFallsBack() {
        AgentLoop loop = loop();
        when(gateway.chat(any())).thenReturn(answer("Kafka is a distributed log."));

        ObjectNode out = loop.run(run);

        assertEquals("Kafka is a distributed log.", out.get("answer").asText());
        assertEquals(0, out.get("sources").size());
    }

    @Test
    void finalAnswerInsideJsonFenceIsParsed() {
        AgentLoop loop = loop();
        when(gateway.chat(any())).thenReturn(answer("```json\n{\"answer\":\"ok\",\"sources\":[]}\n```"));

        assertEquals("ok", loop.run(run).get("answer").asText());
    }
}
```

- [ ] **Step 2: 运行测试，确认编译失败**

Run: `mvn -B -q -pl agentflow-worker -am test -Dtest=AgentLoopTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 编译失败（`AgentLoop` / `AgentRun` 不存在）。

- [ ] **Step 3: 实现**

`AgentRun.java`：

```java
package com.agentflow.worker.agent;

/** 一次 agent 运行的身份与输入:taskId/subtaskId 用于 trace,subtaskUuid 用作存档 key。 */
public record AgentRun(Long taskId, Long subtaskId, String subtaskUuid, String question, String model) {}
```

`AgentLoop.java`：

```java
package com.agentflow.worker.agent;

import com.agentflow.common.llm.ChatMessage;
import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import com.agentflow.common.llm.ToolCall;
import com.agentflow.common.llm.ToolSpec;
import com.agentflow.common.trace.TraceStage;
import com.agentflow.worker.agent.tool.AgentTool;
import com.agentflow.worker.llm.LlmGateway;
import com.agentflow.worker.llm.LlmModelProperties;
import com.agentflow.worker.trace.TraceEmitter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * ReAct 循环:思考 → 调工具 → 看结果 → … → 给出带来源的结论。
 *
 * <ul>
 *   <li>每一步完整结束(LLM 决策 + 全部工具结果)后写 {@link AgentCheckpoint};重新投递时从下一步续跑,
 *       已调用过的工具直接复用存档结果,不重复调用。</li>
 *   <li>工具出错不抛:以 "ERROR: ..." 回给模型让它自己纠正;只有 LLM 调用失败才让子任务进入重试。</li>
 *   <li>同一次运行内相同 name+参数的调用走缓存,防止模型原地打转。</li>
 *   <li>最终 sources 只保留本次运行工具真实返回过的 URL(防编造引用),删掉的发 CITATION_DROPPED。</li>
 *   <li>最多 {@link #MAX_STEPS} 步,超出则去掉工具强制作答。</li>
 * </ul>
 * ponytail: 上下文靠"单个工具结果截断 2000 字 × 最多 6 步"封顶,没有按 token 预算裁剪历史
 * (tool_call 与结果必须成对,不能像 AgentContext 那样逐条丢);步数调大时再加摘要压缩。
 */
@Slf4j
@Component
public class AgentLoop {

    static final int MAX_STEPS = 6;
    static final int MAX_TOKENS = 800;
    static final int PREVIEW_CHARS = 300;
    static final String ERROR = "ERROR: ";
    static final String WORKER_ID = hostname();
    private static final ObjectMapper OM = new ObjectMapper();

    private final LlmGateway gateway;
    private final Map<String, AgentTool> tools;
    private final List<ToolSpec> specs;
    private final AgentCheckpoint checkpoint;
    private final TraceEmitter trace;
    private final LlmModelProperties props;

    public AgentLoop(LlmGateway gateway, List<AgentTool> toolList, AgentCheckpoint checkpoint,
                     TraceEmitter trace, LlmModelProperties props) {
        this.gateway = gateway;
        this.tools = toolList.stream().collect(Collectors.toMap(AgentTool::name, Function.identity()));
        this.specs = toolList.stream()
                .map(t -> new ToolSpec(t.name(), t.description(), t.parametersSchema())).toList();
        this.checkpoint = checkpoint;
        this.trace = trace;
        this.props = props;
    }

    public ObjectNode run(AgentRun run) {
        List<ChatMessage> messages = new ArrayList<>();
        messages.add(new ChatMessage("system", Prompts.AGENT));
        messages.add(new ChatMessage("user", run.question()));
        Map<String, String> cache = new HashMap<>();
        Set<String> seenUrls = new LinkedHashSet<>();
        Usage usage = new Usage();

        // 1) 续跑:把已完成的步骤原样放回对话,工具结果进缓存,不重新调用
        List<AgentStep> done = checkpoint.load(run.subtaskUuid());
        for (AgentStep s : done) {
            messages.add(s.assistant());
            messages.addAll(s.toolResults());
            for (int i = 0; i < s.toolResults().size(); i++) {
                String out = s.toolResults().get(i).getContent();
                if (!out.startsWith(ERROR)) {
                    cache.put(cacheKey(s.assistant().getToolCalls().get(i)), out);
                }
                seenUrls.addAll(TextUtil.urls(out));
            }
            usage.add(s.usage());
        }
        if (!done.isEmpty()) {
            log.info("agent 从存档续跑 subtaskUuid={} fromStep={}", run.subtaskUuid(), done.size());
            emit(run, TraceStage.RESUMED, OM.createObjectNode()
                    .put("fromStep", done.size()).put("workerId", WORKER_ID));
        }

        // 2) ReAct 主循环
        for (int step = done.size() + 1; step <= MAX_STEPS; step++) {
            ChatResponse resp = gateway.chat(request(run.model(), messages, specs));
            Usage stepUsage = Usage.of(resp, props);
            usage.add(stepUsage);
            if (resp.getToolCalls().isEmpty()) {
                return finish(run, step, resp.getContent(), seenUrls, usage);
            }
            ChatMessage assistant = ChatMessage.assistant(resp.getContent(), resp.getToolCalls());
            List<ChatMessage> results = new ArrayList<>();
            ArrayNode calls = OM.createArrayNode();
            for (ToolCall c : resp.getToolCalls()) {
                String out = execute(c, cache);
                seenUrls.addAll(TextUtil.urls(out));
                results.add(ChatMessage.tool(c.getId(), out));
                calls.addObject().put("tool", c.getName()).put("args", c.getArgumentsJson())
                        .put("resultPreview", TextUtil.truncate(out, PREVIEW_CHARS));
            }
            messages.add(assistant);
            messages.addAll(results);
            // 一步完整结束才存档:崩溃在此之前 → 该步重做(工具只读,重做无副作用)
            checkpoint.append(run.subtaskUuid(), new AgentStep(step, assistant, results, stepUsage));
            ObjectNode detail = OM.createObjectNode().put("step", step)
                    .put("thought", resp.getContent()).put("workerId", WORKER_ID);
            detail.set("calls", calls);
            emit(run, TraceStage.AGENT_STEP, detail);
        }

        // 3) 步数耗尽:去掉工具,强制作答(防死循环)
        messages.add(new ChatMessage("user", Prompts.FORCE_ANSWER));
        ChatResponse resp = gateway.chat(request(run.model(), messages, null));
        usage.add(Usage.of(resp, props));
        return finish(run, MAX_STEPS + 1, resp.getContent(), seenUrls, usage);
    }

    private String execute(ToolCall c, Map<String, String> cache) {
        String key = cacheKey(c);
        String hit = cache.get(key);
        if (hit != null) {
            return hit;
        }
        AgentTool tool = tools.get(c.getName());
        if (tool == null) {
            return ERROR + "未知工具 " + c.getName() + ",可用工具:" + tools.keySet();
        }
        String out;
        try {
            String raw = c.getArgumentsJson();
            JsonNode args = OM.readTree(raw == null || raw.isBlank() ? "{}" : raw);
            out = TextUtil.truncate(tool.run(args), TextUtil.MAX_TOOL_CHARS);
        } catch (Exception e) {
            return ERROR + c.getName() + " 调用失败:" + e.getMessage();   // 不缓存:瞬时错误允许模型重试
        }
        cache.put(key, out);
        return out;
    }

    private ObjectNode finish(AgentRun run, int step, String content, Set<String> seenUrls, Usage usage) {
        String answer = content == null ? "" : content.trim();
        JsonNode sources = OM.createArrayNode();
        try {
            JsonNode parsed = TextUtil.parseObject(content);
            if (parsed.path("answer").isTextual()) {
                answer = parsed.get("answer").asText();
                sources = parsed.path("sources");
            }
        } catch (IllegalStateException e) {
            // 模型没按 JSON 作答:整段当答案、无来源。宽松兜底,不为格式问题烧重试。
        }
        if (answer.isBlank()) {
            throw new IllegalStateException("agent 最终答案为空");
        }
        ArrayNode kept = OM.createArrayNode();
        ArrayNode dropped = OM.createArrayNode();
        for (JsonNode s : sources) {
            String url = s.path("url").asText("");
            if (seenUrls.contains(url)) {
                kept.addObject().put("title", s.path("title").asText(url)).put("url", url);
            } else {
                dropped.add(url);
            }
        }
        if (!dropped.isEmpty()) {
            ObjectNode d = OM.createObjectNode();
            d.set("urls", dropped);
            emit(run, TraceStage.CITATION_DROPPED, d);
        }
        emit(run, TraceStage.AGENT_STEP, OM.createObjectNode().put("step", step).put("final", true)
                .put("answer", TextUtil.truncate(answer, PREVIEW_CHARS)).put("workerId", WORKER_ID));

        ObjectNode out = OM.createObjectNode();
        out.put("question", run.question());
        out.put("answer", answer);
        out.set("sources", kept);
        out.set("usage", OM.valueToTree(usage));
        out.put("steps", step);
        return out;
    }

    /** 拷贝 messages:后续追加不影响已发出的请求。 */
    private static ChatRequest request(String model, List<ChatMessage> messages, List<ToolSpec> tools) {
        ChatRequest r = new ChatRequest(model, new ArrayList<>(messages), MAX_TOKENS);
        r.setTools(tools);
        return r;
    }

    private static String cacheKey(ToolCall c) {
        return c.getName() + " " + c.getArgumentsJson();
    }

    private void emit(AgentRun run, TraceStage stage, ObjectNode detail) {
        trace.emit(String.valueOf(run.taskId()), run.taskId(), run.subtaskId(), stage, null, detail.toString());
    }

    private static String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }
}
```

- [ ] **Step 4: 运行测试，确认通过**

Run: `mvn -B -q -pl agentflow-worker -am test -Dtest=AgentLoopTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 10 个测试全部 PASS。

- [ ] **Step 5: commit**

```bash
git add agentflow-worker
git commit -m "feat(agent): AgentLoop ReAct 循环(存档续跑/工具去重/防编造引用/步数上限)"
```

---

### Task 6: 三个 processor 与监听器接线

**Files:**
- Modify: `agentflow-worker/src/main/java/com/agentflow/worker/processor/SubtaskProcessor.java`
- Modify: `agentflow-worker/src/main/java/com/agentflow/worker/listener/SubtaskListener.java`（`processor.process(msg.getInputJson())` → `processor.process(msg)`）
- Modify: `agentflow-worker/src/test/java/com/agentflow/worker/listener/SubtaskListenerIdempotencyTest.java`
- Create: `agentflow-worker/src/main/java/com/agentflow/worker/processor/PlanProcessor.java`
- Create: `agentflow-worker/src/main/java/com/agentflow/worker/processor/AgentProcessor.java`
- Create: `agentflow-worker/src/main/java/com/agentflow/worker/processor/SynthesizeProcessor.java`
- Modify: `agentflow-worker/src/main/resources/application.yml`
- Test: `agentflow-worker/src/test/java/com/agentflow/worker/processor/PlanProcessorTest.java`
- Test: `agentflow-worker/src/test/java/com/agentflow/worker/processor/SynthesizeProcessorTest.java`
- Test: `agentflow-worker/src/test/java/com/agentflow/worker/processor/AgentProcessorTest.java`

**Interfaces:**
- Consumes: `AgentLoop.run(AgentRun)`、`SchemaValidator.validate(JsonNode, Map<String,String>) → List<String>`、`Usage`、`TextUtil`、`Prompts`
- Produces（server 端 Task 7 依赖这些输出格式）：
  - `PLAN` 的输出：`{subQuestions:[string，1~4 个，已去重], usage}`
  - `AGENT` 的输出：`AgentLoop.run` 的结果，即 `{question, answer, sources, usage, steps}`
  - `SYNTHESIZE` 的输入：`{question, model?, findings:[{subQuestion, answer, sources:[{title,url}]}]}`；输出：`{report, sources:[{n,title,url}], usage}`
  - `SubtaskProcessor`：新增 `default String process(SubtaskMessage msg) throws Exception { return process(msg.getInputJson()); }`

- [ ] **Step 1: 写失败的测试**

`PlanProcessorTest.java`：

```java
package com.agentflow.worker.processor;

import com.agentflow.common.llm.ChatResponse;
import com.agentflow.common.llm.TokenUsage;
import com.agentflow.worker.llm.LlmGateway;
import com.agentflow.worker.llm.LlmModelProperties;
import com.agentflow.worker.skill.SchemaValidator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class PlanProcessorTest {

    private final LlmGateway gateway = mock(LlmGateway.class);

    private PlanProcessor processor() {
        LlmModelProperties props = new LlmModelProperties();
        LlmModelProperties.Model m = new LlmModelProperties.Model();
        m.setName("m");
        props.setModels(List.of(m));
        return new PlanProcessor(gateway, props, new SchemaValidator());
    }

    private JsonNode run(String llmOutput) throws Exception {
        when(gateway.chat(any())).thenReturn(new ChatResponse(llmOutput, new TokenUsage(5, 5), "m"));
        return new ObjectMapper().readTree(processor().process("{\"question\":\"kafka vs rabbitmq\"}"));
    }

    @Test
    void dedupesTrimsAndCapsAtFour() throws Exception {
        JsonNode out = run("```json\n{\"subQuestions\":[\" a \",\"a\",\"b\",\"\",\"c\",\"d\",\"e\"]}\n```");
        assertEquals(List.of("a", "b", "c", "d"),
                new ObjectMapper().convertValue(out.get("subQuestions"), List.class));
        assertEquals(1, out.at("/usage/calls").asInt());
    }

    @Test
    void emptyPlanFailsSoItIsRetried() {
        assertThrows(IllegalStateException.class, () -> run("{\"subQuestions\":[]}"));
        assertThrows(IllegalStateException.class, () -> run("{\"questions\":[\"a\"]}"));
        assertThrows(IllegalStateException.class, () -> run("I think you should research Kafka."));
    }

    @Test
    void blankQuestionRejected() {
        assertThrows(IllegalArgumentException.class, () -> processor().process("{\"question\":\"  \"}"));
    }
}
```

`SynthesizeProcessorTest.java`：

```java
package com.agentflow.worker.processor;

import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import com.agentflow.common.llm.TokenUsage;
import com.agentflow.worker.llm.LlmGateway;
import com.agentflow.worker.llm.LlmModelProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class SynthesizeProcessorTest {

    private final LlmGateway gateway = mock(LlmGateway.class);

    private SynthesizeProcessor processor() {
        LlmModelProperties props = new LlmModelProperties();
        LlmModelProperties.Model m = new LlmModelProperties.Model();
        m.setName("m");
        props.setModels(List.of(m));
        return new SynthesizeProcessor(gateway, props);
    }

    private static final String INPUT = """
            {"question":"kafka vs rabbitmq","findings":[
              {"subQuestion":"a","answer":"A1","sources":[{"title":"K","url":"https://k"},{"title":"R","url":"https://r"}]},
              {"subQuestion":"b","answer":"B1","sources":[{"title":"K","url":"https://k"}]}]}
            """;

    @Test
    void numbersDedupedSourcesAndStripsOutOfRangeCitations() throws Exception {
        when(gateway.chat(any())).thenReturn(new ChatResponse("Kafka 吞吐高 [1]。RabbitMQ 路由灵活 [2]。编造 [7]。",
                new TokenUsage(5, 5), "m"));

        JsonNode out = new ObjectMapper().readTree(processor().process(INPUT));

        assertEquals("Kafka 吞吐高 [1]。RabbitMQ 路由灵活 [2]。编造 。", out.get("report").asText());
        assertEquals(2, out.get("sources").size());
        assertEquals(1, out.at("/sources/0/n").asInt());
        assertEquals("https://r", out.at("/sources/1/url").asText());

        ArgumentCaptor<ChatRequest> c = ArgumentCaptor.forClass(ChatRequest.class);
        verify(gateway).chat(c.capture());
        String user = c.getValue().getMessages().get(1).getContent();
        assertTrue(user.contains("[1] K https://k"));
        assertTrue(user.contains("[2] R https://r"));
        assertTrue(user.contains("### a\nA1"));
    }

    @Test
    void blankReportFails() {
        when(gateway.chat(any())).thenReturn(new ChatResponse("  ", new TokenUsage(1, 1), "m"));
        assertThrows(IllegalStateException.class, () -> processor().process(INPUT));
    }
}
```

`AgentProcessorTest.java`：

```java
package com.agentflow.worker.processor;

import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.worker.agent.AgentLoop;
import com.agentflow.worker.agent.AgentRun;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AgentProcessorTest {

    @Test
    void passesSubtaskIdentityToLoop() throws Exception {
        AgentLoop loop = mock(AgentLoop.class);
        when(loop.run(any())).thenReturn(new ObjectMapper().createObjectNode().put("answer", "x"));

        String out = new AgentProcessor(loop).process(
                new SubtaskMessage(7L, 8L, "uuid-8", "AGENT", 1, "{\"question\":\"q\",\"model\":\"m\"}"));

        assertTrue(out.contains("\"answer\":\"x\""));
        verify(loop).run(new AgentRun(7L, 8L, "uuid-8", "q", "m"));
    }

    @Test
    void rawJsonEntryPointIsRejected() {
        assertThrows(UnsupportedOperationException.class,
                () -> new AgentProcessor(mock(AgentLoop.class)).process("{\"question\":\"q\"}"));
    }
}
```

- [ ] **Step 2: 运行测试，确认编译失败**

Run: `mvn -B -q -pl agentflow-worker -am test -Dtest='PlanProcessorTest,SynthesizeProcessorTest,AgentProcessorTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 编译失败。

- [ ] **Step 3: 改 SubtaskProcessor 与监听器**

`SubtaskProcessor.java`（加 import `com.agentflow.common.mq.SubtaskMessage`）：

```java
public interface SubtaskProcessor {
    String type();

    String process(String inputJson) throws Exception;

    /** 需要子任务身份(如按 subtaskUuid 存档续跑)的处理器覆盖此方法;默认只看 input。 */
    default String process(SubtaskMessage msg) throws Exception {
        return process(msg.getInputJson());
    }
}
```

`SubtaskListener.java`：把 `String output = processor.process(msg.getInputJson());` 改为 `String output = processor.process(msg);`。

`SubtaskListenerIdempotencyTest.java`：`verify(processor, never()).process(any());` 改为 `verify(processor, never()).process(anyString());`（新增重载后 `any()` 会产生歧义；按需补 `anyString` 的 import）。

- [ ] **Step 4: 实现三个 processor**

`PlanProcessor.java`：

```java
package com.agentflow.worker.processor;

import com.agentflow.common.llm.ChatMessage;
import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import com.agentflow.worker.agent.Prompts;
import com.agentflow.worker.agent.TextUtil;
import com.agentflow.worker.agent.Usage;
import com.agentflow.worker.llm.LlmGateway;
import com.agentflow.worker.llm.LlmModelProperties;
import com.agentflow.worker.skill.SchemaValidator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** RESEARCH_AGENT 阶段①:LLM 把问题拆成子问题。输出过 schema 校验 + 去重 + 截断到 4 个;空 → 失败重试。 */
@Component
@RequiredArgsConstructor
public class PlanProcessor implements SubtaskProcessor {

    static final int MAX_SUB_QUESTIONS = 4;
    private static final ObjectMapper OM = new ObjectMapper();

    private final LlmGateway gateway;
    private final LlmModelProperties props;
    private final SchemaValidator validator;

    @Override
    public String type() {
        return "PLAN";
    }

    @Override
    public String process(String inputJson) throws Exception {
        JsonNode in = OM.readTree(inputJson);
        String question = in.path("question").asText("").trim();
        if (question.isEmpty()) {
            throw new IllegalArgumentException("input.question 缺失");
        }
        String model = in.hasNonNull("model") ? in.get("model").asText() : null;
        ChatResponse resp = gateway.chat(new ChatRequest(model, List.of(
                new ChatMessage("system", Prompts.PLANNER), new ChatMessage("user", question)), 400));

        JsonNode plan = TextUtil.parseObject(resp.getContent());
        List<String> violations = validator.validate(plan, Map.of("subQuestions", "array"));
        if (!violations.isEmpty()) {
            throw new IllegalStateException("拆题输出不合 schema:" + violations);
        }
        Set<String> subs = new LinkedHashSet<>();
        for (JsonNode n : plan.get("subQuestions")) {
            if (n.isTextual() && !n.asText().isBlank()) {
                subs.add(n.asText().trim());
            }
        }
        if (subs.isEmpty()) {
            throw new IllegalStateException("拆题结果为空");
        }
        ObjectNode out = OM.createObjectNode();
        ArrayNode arr = out.putArray("subQuestions");
        subs.stream().limit(MAX_SUB_QUESTIONS).forEach(arr::add);
        out.set("usage", OM.valueToTree(Usage.of(resp, props)));
        return out.toString();
    }
}
```

`AgentProcessor.java`：

```java
package com.agentflow.worker.processor;

import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.worker.agent.AgentLoop;
import com.agentflow.worker.agent.AgentRun;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/** RESEARCH_AGENT 阶段②:一个子问题一个 ReAct agent。需要 subtaskUuid 做存档 key,故走 process(SubtaskMessage)。 */
@Component
@RequiredArgsConstructor
public class AgentProcessor implements SubtaskProcessor {

    private static final ObjectMapper OM = new ObjectMapper();

    private final AgentLoop loop;

    @Override
    public String type() {
        return "AGENT";
    }

    @Override
    public String process(String inputJson) {
        throw new UnsupportedOperationException("AGENT 需要子任务身份(按 subtaskUuid 存档续跑),请走 process(SubtaskMessage)");
    }

    @Override
    public String process(SubtaskMessage msg) throws Exception {
        JsonNode in = OM.readTree(msg.getInputJson());
        String question = in.path("question").asText("").trim();
        if (question.isEmpty()) {
            throw new IllegalArgumentException("input.question 缺失");
        }
        String model = in.hasNonNull("model") ? in.get("model").asText() : null;
        return loop.run(new AgentRun(msg.getTaskId(), msg.getSubtaskId(), msg.getSubtaskUuid(), question, model))
                .toString();
    }
}
```

`SynthesizeProcessor.java`：

```java
package com.agentflow.worker.processor;

import com.agentflow.common.llm.ChatMessage;
import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import com.agentflow.worker.agent.Prompts;
import com.agentflow.worker.agent.Usage;
import com.agentflow.worker.llm.LlmGateway;
import com.agentflow.worker.llm.LlmModelProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * RESEARCH_AGENT 阶段③:把各 agent 的结论合成一篇带 [n] 引用的报告。
 * 来源按 URL 去重后统一编号;报告里超出编号范围的 [n](编造的引用)直接删掉。
 */
@Component
@RequiredArgsConstructor
public class SynthesizeProcessor implements SubtaskProcessor {

    private static final Pattern CITE = Pattern.compile("\\[(\\d{1,3})]");
    private static final ObjectMapper OM = new ObjectMapper();

    private final LlmGateway gateway;
    private final LlmModelProperties props;

    @Override
    public String type() {
        return "SYNTHESIZE";
    }

    @Override
    public String process(String inputJson) throws Exception {
        JsonNode in = OM.readTree(inputJson);
        String question = in.path("question").asText();
        String model = in.hasNonNull("model") ? in.get("model").asText() : null;

        Map<String, String> urlToTitle = new LinkedHashMap<>();
        StringBuilder findings = new StringBuilder();
        for (JsonNode f : in.path("findings")) {
            findings.append("### ").append(f.path("subQuestion").asText()).append('\n')
                    .append(f.path("answer").asText()).append("\n\n");
            for (JsonNode s : f.path("sources")) {
                String url = s.path("url").asText("");
                if (!url.isEmpty()) {
                    urlToTitle.putIfAbsent(url, s.path("title").asText(url));
                }
            }
        }
        List<Map.Entry<String, String>> sources = new ArrayList<>(urlToTitle.entrySet());
        StringBuilder user = new StringBuilder("问题:").append(question).append("\n\n可引用来源:\n");
        for (int i = 0; i < sources.size(); i++) {
            user.append('[').append(i + 1).append("] ").append(sources.get(i).getValue())
                    .append(' ').append(sources.get(i).getKey()).append('\n');
        }
        user.append("\n各子问题结论:\n").append(findings);

        ChatResponse resp = gateway.chat(new ChatRequest(model, List.of(
                new ChatMessage("system", Prompts.SYNTHESIZER), new ChatMessage("user", user.toString())), 1500));
        String report = stripInvalidCitations(resp.getContent(), sources.size());
        if (report.isBlank()) {
            throw new IllegalStateException("合成报告为空");
        }

        ObjectNode out = OM.createObjectNode().put("report", report);
        ArrayNode arr = out.putArray("sources");
        for (int i = 0; i < sources.size(); i++) {
            arr.addObject().put("n", i + 1).put("title", sources.get(i).getValue()).put("url", sources.get(i).getKey());
        }
        out.set("usage", OM.valueToTree(Usage.of(resp, props)));
        return out.toString();
    }

    static String stripInvalidCitations(String text, int max) {
        return CITE.matcher(text == null ? "" : text).replaceAll(m -> {
            int n = Integer.parseInt(m.group(1));
            return n >= 1 && n <= max ? m.group() : "";
        }).trim();
    }
}
```

- [ ] **Step 5: 改 worker 配置**

在 `agentflow-worker/src/main/resources/application.yml` 中：
1. `spring.kafka.listener` 下新增 `concurrency: 2`。
2. `llm:` 下新增 mock 延迟配置，并在 `models` 列表末尾追加真实模型条目：

```yaml
llm:
  provider: mock
  default-model: mock-small
  # mock 每次调用的模拟耗时(ms):docker compose 里设 1500,便于在页面看清每一步、演示杀 worker
  mock:
    delay-ms: 0
  models:
    # ...原有 mock-small / mock-large 不变...
    - name: ${LLM_MODEL:mock-small}   # 真实模型(.env 的 LLM_MODEL);未设置时与 mock-small 同名,不生效
      capacity: 10
      refill-per-sec: 2
      price-per1k-tokens: ${LLM_PRICE_PER1K:0.001}
```

- [ ] **Step 6: 运行测试，确认通过；再跑 worker 全量回归**

Run: `mvn -B -q -pl agentflow-worker -am test -Dtest='PlanProcessorTest,SynthesizeProcessorTest,AgentProcessorTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS。
Run: `mvn -B -q -pl agentflow-worker -am test 2>&1 | tail -20`
Expected: 全部 PASS（包括 `SubtaskListenerTest`、`SubtaskListenerIdempotencyTest`）。

- [ ] **Step 7: commit**

```bash
git add agentflow-worker
git commit -m "feat(worker): PLAN/AGENT/SYNTHESIZE 三个处理器 + 监听器传递子任务身份"
```

---

### Task 7: server 阶段推进（plan → agents → synthesize）

**Files:**
- Modify: `docker/mysql/init.sql`（`subtask` 表在 `seq` 后加 `kind VARCHAR(16) NULL,`）
- Create: `docker/mysql/migrations/2026-10-15-subtask-kind.sql`
- Modify: `agentflow-server/src/main/java/com/agentflow/server/entity/SubtaskEntity.java`
- Modify: `agentflow-server/src/main/java/com/agentflow/server/coordinator/SubtaskDef.java`
- Modify: `agentflow-server/src/main/java/com/agentflow/server/coordinator/TaskDecomposer.java`
- Create: `agentflow-server/src/main/java/com/agentflow/server/coordinator/ResearchAgentDecomposer.java`
- Create: `agentflow-server/src/main/java/com/agentflow/server/coordinator/StageAdvancer.java`
- Modify: `agentflow-server/src/main/java/com/agentflow/server/coordinator/SubtaskDispatcher.java`
- Modify: `agentflow-server/src/main/java/com/agentflow/server/mapper/TaskMapper.java`
- Modify: `agentflow-server/src/main/java/com/agentflow/server/service/ResultHandleService.java`
- Modify: `agentflow-server/src/main/java/com/agentflow/server/service/TimeoutSweepService.java`
- Modify: `agentflow-server/src/main/java/com/agentflow/server/service/DlqRecoveryService.java`（重投处构造 `SubtaskMessage` 的地方）
- Modify: `agentflow-server/src/main/java/com/agentflow/server/service/TaskSubmitService.java`（persist 中的子任务插入）
- Modify: `agentflow-server/src/main/java/com/agentflow/server/service/TaskFinalizer.java`（aggregate 中输出 kind）
- Test: `agentflow-server/src/test/java/com/agentflow/server/coordinator/ResearchAgentDecomposerTest.java`
- Test: `agentflow-server/src/test/java/com/agentflow/server/service/ResearchAgentFlowTest.java`

**Interfaces:**
- Consumes: Task 6 中 PLAN / AGENT 的输出格式
- Produces:
  - `record SubtaskDef(int seq, String inputJson, String kind)`，保留 `SubtaskDef(int seq, String inputJson)` 构造器（kind 为 null），并提供 `SubtaskEntity toEntity(Long taskId)`
  - `TaskDecomposer.nextStage(TaskEntity, List<SubtaskEntity>) → List<SubtaskDef>`，默认返回空列表
  - `StageAdvancer.onSettled(Long taskId)`：子任务落定后的唯一入口
  - `SubtaskDispatcher.dispatchPending(TaskEntity)`、`static String typeOf(String taskType, SubtaskEntity sub)`
  - `TaskMapper.addTotal(Long id, int n)`
  - trace `STAGE_ADVANCED`：status 为下一阶段的 kind；detail 为 `{kind, subtasks:[{subtaskId, seq, input}]}`（Task 8 的页面依赖这个格式）
  - `task.result` 中每一项都带 `kind` 字段

- [ ] **Step 1: 本地数据库加列**

```bash
mkdir -p docker/mysql/migrations
cat > docker/mysql/migrations/2026-10-15-subtask-kind.sql <<'EOF'
-- RESEARCH_AGENT:子任务可有自己的处理类型(PLAN/AGENT/SYNTHESIZE);旧任务为 NULL,沿用 task.type。
-- 新库由 init.sql 建好,无需执行;已有库执行一次即可。
ALTER TABLE subtask ADD COLUMN kind VARCHAR(16) NULL AFTER seq;
EOF
docker exec -i agentflow-mysql mysql -uroot -pagentflow123 agentflow < docker/mysql/migrations/2026-10-15-subtask-kind.sql
```

Expected：执行成功；如果报 `Duplicate column name 'kind'`，说明已经加过，可以忽略。同时把 `init.sql` 的 `subtask` 表在 `seq INT NOT NULL,` 之后加一行 `kind VARCHAR(16) NULL,`。

- [ ] **Step 2: 写失败的测试**

`ResearchAgentDecomposerTest.java`：

```java
package com.agentflow.server.coordinator;

import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ResearchAgentDecomposerTest {

    private final ObjectMapper om = new ObjectMapper();
    private final ResearchAgentDecomposer d = new ResearchAgentDecomposer(om);

    private static SubtaskEntity sub(int seq, String kind, String status, String input, String output) {
        SubtaskEntity s = new SubtaskEntity();
        s.setSeq(seq);
        s.setKind(kind);
        s.setStatus(status);
        s.setInput(input);
        s.setOutput(output);
        return s;
    }

    private static TaskEntity task() {
        TaskEntity t = new TaskEntity();
        t.setType("RESEARCH_AGENT");
        t.setPayload("{\"question\":\"kafka vs rabbitmq\"}");
        return t;
    }

    @Test
    void decomposeCreatesSinglePlanSubtask() throws Exception {
        List<SubtaskDef> defs = d.decompose(om.readTree("{\"question\":\"  kafka vs rabbitmq \"}"));
        assertEquals(1, defs.size());
        assertEquals("PLAN", defs.get(0).kind());
        assertEquals("kafka vs rabbitmq", om.readTree(defs.get(0).inputJson()).get("question").asText());
    }

    @Test
    void decomposeValidatesQuestion() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> d.decompose(om.readTree("{}")));
        assertThrows(IllegalArgumentException.class, () -> d.decompose(om.readTree("{\"question\":\" \"}")));
        assertThrows(IllegalArgumentException.class,
                () -> d.decompose(om.readTree("{\"question\":\"" + "x".repeat(501) + "\"}")));
        assertThrows(IllegalArgumentException.class, () -> d.decompose(null));
    }

    @Test
    void completedPlanFansOutAgents() throws Exception {
        List<SubtaskDef> next = d.nextStage(task(), List.of(
                sub(0, "PLAN", "COMPLETED", "{}", "{\"subQuestions\":[\"a\",\"b\",\"c\"]}")));
        assertEquals(3, next.size());
        assertEquals(List.of(1, 2, 3), next.stream().map(SubtaskDef::seq).toList());
        assertTrue(next.stream().allMatch(x -> "AGENT".equals(x.kind())));
        assertEquals("b", om.readTree(next.get(1).inputJson()).get("question").asText());
    }

    @Test
    void failedPlanStops() {
        assertTrue(d.nextStage(task(), List.of(sub(0, "PLAN", "FAILED", "{}", null))).isEmpty());
    }

    @Test
    void settledAgentsProduceOneSynthesizeWithSuccessfulFindingsOnly() throws Exception {
        List<SubtaskDef> next = d.nextStage(task(), List.of(
                sub(0, "PLAN", "COMPLETED", "{}", "{\"subQuestions\":[\"a\",\"b\"]}"),
                sub(1, "AGENT", "COMPLETED", "{\"question\":\"a\"}",
                        "{\"answer\":\"A1\",\"sources\":[{\"title\":\"K\",\"url\":\"https://k\"}]}"),
                sub(2, "AGENT", "FAILED", "{\"question\":\"b\"}", null)));
        assertEquals(1, next.size());
        assertEquals("SYNTHESIZE", next.get(0).kind());
        assertEquals(3, next.get(0).seq());
        var in = om.readTree(next.get(0).inputJson());
        assertEquals("kafka vs rabbitmq", in.get("question").asText());
        assertEquals(1, in.get("findings").size());
        assertEquals("a", in.at("/findings/0/subQuestion").asText());
        assertEquals("https://k", in.at("/findings/0/sources/0/url").asText());
    }

    @Test
    void allAgentsFailedOrSynthesizeExistsStops() {
        assertTrue(d.nextStage(task(), List.of(
                sub(0, "PLAN", "COMPLETED", "{}", "{\"subQuestions\":[\"a\"]}"),
                sub(1, "AGENT", "FAILED", "{\"question\":\"a\"}", null))).isEmpty());
        assertTrue(d.nextStage(task(), List.of(
                sub(0, "PLAN", "COMPLETED", "{}", "{\"subQuestions\":[\"a\"]}"),
                sub(1, "AGENT", "COMPLETED", "{\"question\":\"a\"}", "{\"answer\":\"x\",\"sources\":[]}"),
                sub(2, "SYNTHESIZE", "COMPLETED", "{}", "{\"report\":\"r\"}"))).isEmpty());
    }
}
```

`ResearchAgentFlowTest.java`（真 MySQL，和现有 `ResultHandleServiceTest` 一样的写法）：

```java
package com.agentflow.server.service;

import com.agentflow.common.mq.ResultMessage;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RESEARCH_AGENT 三阶段推进(经 ResultHandleService 真实入口)。
 * 测试事务最终回滚,afterCommit 的分发不会触发 → 新阶段子任务停在 PENDING,便于断言。
 */
@SpringBootTest
@Transactional
class ResearchAgentFlowTest {

    @Autowired private ResultHandleService service;
    @Autowired private TaskMapper taskMapper;
    @Autowired private SubtaskMapper subtaskMapper;
    @MockBean private KafkaTemplate<String, Object> kafkaTemplate;

    /** RUNNING 的 RESEARCH_AGENT 任务 + 1 个 DISPATCHED 的 PLAN 子任务,返回 [taskId, planId]。 */
    private long[] fixture() {
        TaskEntity t = new TaskEntity();
        t.setTaskUuid(UUID.randomUUID().toString());
        t.setType("RESEARCH_AGENT");
        t.setStatus("RUNNING");
        t.setPayload("{\"question\":\"kafka vs rabbitmq\"}");
        t.setSubtaskTotal(1);
        t.setSubtaskDone(0);
        t.setSubtaskFailed(0);
        taskMapper.insert(t);
        SubtaskEntity p = new SubtaskEntity();
        p.setSubtaskUuid(UUID.randomUUID().toString());
        p.setTaskId(t.getId());
        p.setSeq(0);
        p.setKind("PLAN");
        p.setStatus("DISPATCHED");
        p.setInput("{\"question\":\"kafka vs rabbitmq\"}");
        subtaskMapper.insert(p);
        return new long[]{t.getId(), p.getId()};
    }

    private List<SubtaskEntity> subs(long taskId, String kind) {
        return subtaskMapper.selectList(new QueryWrapper<SubtaskEntity>()
                .eq("task_id", taskId).eq("kind", kind).orderByAsc("seq"));
    }

    private void dispatched(SubtaskEntity s) {
        subtaskMapper.casStatus(s.getId(), "PENDING", "DISPATCHED");
    }

    @Test
    void planThenAgentsThenSynthesizeThenPartialFailed() {
        long[] ids = fixture();
        long taskId = ids[0];

        service.handle(new ResultMessage(taskId, ids[1], true, "{\"subQuestions\":[\"a\",\"b\"]}", null));
        TaskEntity t = taskMapper.selectById(taskId);
        assertEquals("RUNNING", t.getStatus());
        assertEquals(3, t.getSubtaskTotal());
        List<SubtaskEntity> agents = subs(taskId, "AGENT");
        assertEquals(2, agents.size());
        assertEquals("PENDING", agents.get(0).getStatus());

        agents.forEach(this::dispatched);
        service.handle(new ResultMessage(taskId, agents.get(0).getId(), true,
                "{\"answer\":\"A1\",\"sources\":[]}", null));
        assertTrue(subs(taskId, "SYNTHESIZE").isEmpty());   // 还有 agent 没落定,不能合成

        service.handle(new ResultMessage(taskId, agents.get(1).getId(), false, null, "boom"));
        List<SubtaskEntity> synth = subs(taskId, "SYNTHESIZE");
        assertEquals(1, synth.size());
        assertEquals(3, synth.get(0).getSeq());
        assertEquals(4, taskMapper.selectById(taskId).getSubtaskTotal());
        assertEquals("RUNNING", taskMapper.selectById(taskId).getStatus());

        dispatched(synth.get(0));
        service.handle(new ResultMessage(taskId, synth.get(0).getId(), true,
                "{\"report\":\"R [1]\",\"sources\":[]}", null));
        t = taskMapper.selectById(taskId);
        assertEquals("PARTIAL_FAILED", t.getStatus());   // 一个 agent 失败:部分失败可见,报告仍在结果里
        assertTrue(t.getResult().contains("\"report\""));
        assertTrue(t.getResult().contains("\"kind\":\"SYNTHESIZE\""));
    }

    @Test
    void failedPlanEndsTaskFailedWithoutAgents() {
        long[] ids = fixture();
        service.handle(new ResultMessage(ids[0], ids[1], false, null, "LLM down"));
        assertEquals("FAILED", taskMapper.selectById(ids[0]).getStatus());
        assertTrue(subs(ids[0], "AGENT").isEmpty());
    }
}
```

- [ ] **Step 3: 运行测试，确认编译失败**

Run: `mvn -B -q -pl agentflow-server -am test -Dtest='ResearchAgentDecomposerTest,ResearchAgentFlowTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 编译失败。

- [ ] **Step 4: 实现数据层改动**

`SubtaskEntity.java`：在 `seq` 后加：

```java
    private String kind;         // 子任务处理类型(PLAN/AGENT/SYNTHESIZE);null = 沿用 task.type
```

`SubtaskDef.java` 整体替换：

```java
package com.agentflow.server.coordinator;

import com.agentflow.common.state.SubtaskStatus;
import com.agentflow.server.entity.SubtaskEntity;

import java.util.UUID;

/** 拆解产物:一个待创建子任务的定义。kind 为空 → worker 按任务类型路由(旧任务类型)。 */
public record SubtaskDef(int seq, String inputJson, String kind) {

    public SubtaskDef(int seq, String inputJson) {
        this(seq, inputJson, null);
    }

    public SubtaskEntity toEntity(Long taskId) {
        SubtaskEntity sub = new SubtaskEntity();
        sub.setSubtaskUuid(UUID.randomUUID().toString());
        sub.setTaskId(taskId);
        sub.setSeq(seq);
        sub.setKind(kind);
        sub.setStatus(SubtaskStatus.PENDING.name());
        sub.setInput(inputJson);
        return sub;
    }
}
```

`TaskSubmitService.persist`：把循环内构造 `SubtaskEntity` 的 7 行替换为 `subtaskMapper.insert(def.toEntity(task.getId()));`。

`TaskMapper.java` 追加：

```java
    /** 阶段推进:新增下一阶段子任务时原子调大 total(与落定计数同事务,防提前判终态)。 */
    @Update("UPDATE task SET subtask_total = subtask_total + #{n} WHERE id = #{id}")
    int addTotal(@Param("id") Long id, @Param("n") int n);
```

`TaskDecomposer.java` 整体替换：

```java
package com.agentflow.server.coordinator;

import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

/** 协调器拆解策略:每种任务类型一个实现,Spring 自动注册(策略模式)。 */
public interface TaskDecomposer {
    String type();

    List<SubtaskDef> decompose(JsonNode payload);

    /**
     * 当前所有子任务都已落定时调用:返回下一阶段要创建的子任务;空 = 没有下一阶段,任务进入终态。
     * 单阶段任务类型不必覆盖。
     */
    default List<SubtaskDef> nextStage(TaskEntity task, List<SubtaskEntity> subs) {
        return List.of();
    }
}
```

`TaskFinalizer.aggregate`：在 `item.put("status", s.getStatus());` 之后加：

```java
            if (s.getKind() != null) {
                item.put("kind", s.getKind());
            }
```

- [ ] **Step 5: 实现 ResearchAgentDecomposer**

```java
package com.agentflow.server.coordinator;

import com.agentflow.common.state.SubtaskStatus;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * RESEARCH_AGENT:plan → map → reduce 三阶段。
 * 提交时只建 1 个 PLAN;PLAN 成功 → N 个 AGENT(并行);AGENT 全部落定且至少 1 个成功 → 1 个 SYNTHESIZE。
 * 每个阶段的每个子任务都是普通 Kafka 子任务,复用重试 / DLQ / 超时兜底 / 幂等 / trace。
 */
@Component
@RequiredArgsConstructor
public class ResearchAgentDecomposer implements TaskDecomposer {

    static final int MAX_QUESTION_CHARS = 500;
    static final String PLAN = "PLAN";
    static final String AGENT = "AGENT";
    static final String SYNTHESIZE = "SYNTHESIZE";

    private final ObjectMapper objectMapper;

    @Override
    public String type() {
        return "RESEARCH_AGENT";
    }

    @Override
    public List<SubtaskDef> decompose(JsonNode payload) {
        String question = payload == null ? "" : payload.path("question").asText("").trim();
        if (question.isEmpty()) {
            throw new IllegalArgumentException("payload.question 不能为空");
        }
        if (question.length() > MAX_QUESTION_CHARS) {
            throw new IllegalArgumentException("payload.question 不能超过 " + MAX_QUESTION_CHARS + " 字");
        }
        return List.of(new SubtaskDef(0, input(question, model(payload)).toString(), PLAN));
    }

    @Override
    public List<SubtaskDef> nextStage(TaskEntity task, List<SubtaskEntity> subs) {
        if (subs.stream().anyMatch(s -> SYNTHESIZE.equals(s.getKind()))) {
            return List.of();
        }
        JsonNode payload = json(task.getPayload());
        String model = model(payload);
        List<SubtaskEntity> agents = subs.stream().filter(s -> AGENT.equals(s.getKind())).toList();

        if (agents.isEmpty()) {   // 阶段①刚结束:按拆题结果扇出 agent
            SubtaskEntity plan = subs.stream().filter(s -> PLAN.equals(s.getKind())).findFirst().orElse(null);
            if (plan == null || !SubtaskStatus.COMPLETED.name().equals(plan.getStatus())) {
                return List.of();
            }
            List<SubtaskDef> defs = new ArrayList<>();
            int seq = 1;
            for (JsonNode q : json(plan.getOutput()).path("subQuestions")) {
                defs.add(new SubtaskDef(seq++, input(q.asText(), model).toString(), AGENT));
            }
            return defs;
        }

        // 阶段②结束:汇总成功的 agent 结论,交给合成
        ArrayNode findings = objectMapper.createArrayNode();
        int maxSeq = 0;
        for (SubtaskEntity a : agents) {
            maxSeq = Math.max(maxSeq, a.getSeq());
            if (!SubtaskStatus.COMPLETED.name().equals(a.getStatus())) {
                continue;
            }
            JsonNode out = json(a.getOutput());
            ObjectNode f = findings.addObject()
                    .put("subQuestion", json(a.getInput()).path("question").asText())
                    .put("answer", out.path("answer").asText());
            f.set("sources", out.path("sources"));
        }
        if (findings.isEmpty()) {
            return List.of();
        }
        ObjectNode in = input(payload.path("question").asText(), model);
        in.set("findings", findings);
        return List.of(new SubtaskDef(maxSeq + 1, in.toString(), SYNTHESIZE));
    }

    private ObjectNode input(String question, String model) {
        ObjectNode n = objectMapper.createObjectNode().put("question", question);
        if (model != null) {
            n.put("model", model);
        }
        return n;
    }

    private static String model(JsonNode payload) {
        return payload != null && payload.hasNonNull("model") ? payload.get("model").asText() : null;
    }

    private JsonNode json(String s) {
        try {
            return objectMapper.readTree(s == null ? "{}" : s);
        } catch (Exception e) {
            throw new IllegalStateException("JSON 解析失败:" + s, e);
        }
    }
}
```

- [ ] **Step 6: 实现 StageAdvancer，改分发器**

`SubtaskDispatcher.java`：把现有的 `dispatch` 拆成两个方法，再加一个 `typeOf`：

```java
    public void dispatch(TaskEntity task) {
        if (!stateMachine.transitionTask(task.getId(), TaskStatus.PENDING, TaskStatus.RUNNING)) {
            log.warn("任务已被并发分发,跳过 taskId={}", task.getId());
            return;
        }
        dispatchPending(task);
    }

    /** 发送该任务所有 PENDING 子任务(提交时 / 阶段推进后共用;不做 task 状态迁移)。 */
    public void dispatchPending(TaskEntity task) {
        List<SubtaskEntity> subs = subtaskMapper.selectList(
                new QueryWrapper<SubtaskEntity>()
                        .eq("task_id", task.getId())
                        .eq("status", SubtaskStatus.PENDING.name()));
        for (SubtaskEntity sub : subs) {
            SubtaskMessage msg = new SubtaskMessage(
                    task.getId(), sub.getId(), sub.getSubtaskUuid(),
                    typeOf(task.getType(), sub), sub.getSeq(), sub.getInput());
            // ...原有 try { send().get(); transitionSubtask; trace } catch {...} 原样保留...
        }
    }

    /** worker 按此路由处理器:子任务有自己的 kind 就用 kind,否则沿用任务类型。 */
    public static String typeOf(String taskType, SubtaskEntity sub) {
        return sub.getKind() != null ? sub.getKind() : taskType;
    }
```

`TimeoutSweepService`：重投处的 `type` 改为 `SubtaskDispatcher.typeOf(taskMapper.selectById(s.getTaskId()).getType(), s)`；超时失败落定处的 `taskFinalizer.finalizeIfAllSettled(s.getTaskId());` 改为 `stageAdvancer.onSettled(s.getTaskId());`。字段 `TaskFinalizer taskFinalizer` 换成 `StageAdvancer stageAdvancer`，同步更新类注释中「终态落定复用 TaskFinalizer」一句为「落定后统一经 StageAdvancer(推进下一阶段或判终态)」。

`DlqRecoveryService`（重投处）：`t.getType()` 改为 `SubtaskDispatcher.typeOf(t.getType(), s)`（补 import）。

`ResultHandleService`：字段 `TaskFinalizer taskFinalizer` 换成 `StageAdvancer stageAdvancer`；最后一行改为 `stageAdvancer.onSettled(msg.getTaskId());`；类注释中「终态判定 + 聚合逻辑抽到 TaskFinalizer」一句后补「;落定后先经 StageAdvancer 判断是否有下一阶段」。

`StageAdvancer.java`：

```java
package com.agentflow.server.coordinator;

import com.agentflow.common.trace.TraceStage;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import com.agentflow.server.service.TaskFinalizer;
import com.agentflow.server.trace.TraceEmitter;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 子任务落定后的唯一入口(结果回传、超时兜底两条触发源共用):
 * 当前全部落定且任务类型有下一阶段 → 同一事务内建下一阶段子任务并调大 total,提交后再分发;
 * 否则交 {@link TaskFinalizer} 判终态。
 *
 * <p>并发:计数累加是对 task 行的 UPDATE,持行锁到事务提交,多个结果并发回来时串行化;
 * 只有最后一个看到 done+failed==total,所以下一阶段只会创建一次。
 */
@Slf4j
@Component
public class StageAdvancer {

    private final Map<String, TaskDecomposer> decomposers;
    private final TaskMapper taskMapper;
    private final SubtaskMapper subtaskMapper;
    private final SubtaskDispatcher dispatcher;
    private final TaskFinalizer taskFinalizer;
    private final TraceEmitter traceEmitter;
    private final ObjectMapper objectMapper;

    public StageAdvancer(List<TaskDecomposer> decomposerList, TaskMapper taskMapper, SubtaskMapper subtaskMapper,
                         SubtaskDispatcher dispatcher, TaskFinalizer taskFinalizer,
                         TraceEmitter traceEmitter, ObjectMapper objectMapper) {
        this.decomposers = decomposerList.stream()
                .collect(Collectors.toMap(TaskDecomposer::type, Function.identity()));
        this.taskMapper = taskMapper;
        this.subtaskMapper = subtaskMapper;
        this.dispatcher = dispatcher;
        this.taskFinalizer = taskFinalizer;
        this.traceEmitter = traceEmitter;
        this.objectMapper = objectMapper;
    }

    public void onSettled(Long taskId) {
        TaskEntity task = taskMapper.selectById(taskId);
        if (task.getSubtaskDone() + task.getSubtaskFailed() == task.getSubtaskTotal()) {
            TaskDecomposer decomposer = decomposers.get(task.getType());
            if (decomposer != null) {
                List<SubtaskEntity> subs = subtaskMapper.selectList(new QueryWrapper<SubtaskEntity>()
                        .eq("task_id", taskId).orderByAsc("seq"));
                List<SubtaskDef> next = decomposer.nextStage(task, subs);
                if (!next.isEmpty()) {
                    advance(task, next);
                    return;
                }
            }
        }
        taskFinalizer.finalizeIfAllSettled(taskId);
    }

    private void advance(TaskEntity task, List<SubtaskDef> next) {
        ArrayNode created = objectMapper.createArrayNode();
        for (SubtaskDef def : next) {
            SubtaskEntity sub = def.toEntity(task.getId());
            subtaskMapper.insert(sub);
            ObjectNode item = created.addObject().put("subtaskId", sub.getId()).put("seq", def.seq());
            try {
                item.set("input", objectMapper.readTree(def.inputJson()));
            } catch (Exception e) {
                item.put("input", def.inputJson());
            }
        }
        // 与本次结果落定同一事务:total 先变大,TaskFinalizer 不会把任务提前判终态
        taskMapper.addTotal(task.getId(), next.size());
        String kind = next.get(0).kind();
        ObjectNode detail = objectMapper.createObjectNode().put("kind", kind);
        detail.set("subtasks", created);
        traceEmitter.emit(String.valueOf(task.getId()), task.getId(), null,
                TraceStage.STAGE_ADVANCED, kind, detail.toString());
        log.info("任务进入下一阶段 taskId={} kind={} count={}", task.getId(), kind, next.size());

        // 事务提交后才发 MQ:否则 worker 可能先于提交收到消息,回传结果时查不到子任务
        Runnable dispatch = () -> dispatcher.dispatchPending(task);
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    dispatch.run();
                }
            });
        } else {
            dispatch.run();
        }
    }
}
```

- [ ] **Step 7: 运行测试，确认通过；再跑 server 全量回归**

Run: `mvn -B -q -pl agentflow-server -am test -Dtest='ResearchAgentDecomposerTest,ResearchAgentFlowTest' -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS。
Run: `mvn -B -q -pl agentflow-server -am test 2>&1 | tail -20`
Expected: 全部 PASS（`ResultHandleServiceTest`、`TimeoutSweepServiceTest`、`DlqRecoveryServiceTest`、`SubtaskDispatcherTest` 都不受影响）。

- [ ] **Step 8: commit**

```bash
git add docker/mysql agentflow-server
git commit -m "feat(server): RESEARCH_AGENT 三阶段推进(StageAdvancer:同事务建下一阶段,提交后分发)"
```

---

### Task 8: 控制台（Research / Ops 两个页签）

**Files:**
- Modify: `agentflow-server/src/main/resources/static/index.html`

**Interfaces:**
- Consumes: `POST /api/v1/tasks`（`{type:"RESEARCH_AGENT", payload:{question}}`）、`GET /api/v1/tasks/{uuid}`（`status`、`subtasks[{seq,status,error}]`、`result[{seq,kind,status,output,error}]`）、`GET /api/v1/tasks/{uuid}/trace`（`[{stage,status,subtaskId,detail,timestamp}]`），以及 Task 5、Task 7 中定义的 trace detail 格式

- [ ] **Step 1: 修 Ops 页签已有的 XSS 隐患**

Ops 页签的 `renderTrace` 和 `loadUsage` 用 `innerHTML` 拼接了 `e.detail` 和 `r.model`。现在 detail 里会出现网页和模型的文本，必须转义。在 `<script>` 开头的 `const $=...` 之后加：

```js
const esc=s=>String(s??"").replace(/[&<>"']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",'"':"&quot;","'":"&#39;"}[c]));
```

然后：`renderTrace` 中 `${e.stage}` 改为 `${esc(e.stage)}`，`${meta?" — "+meta:""}` 改为 `${meta?" — "+esc(meta):""}`；`loadUsage` 中 `${r.model}` 改为 `${esc(r.model)}`。

- [ ] **Step 2: 加页签与 Research 视图**

1. `<style>` 末尾追加：

```css
  [hidden]{display:none!important}
  .tabs{margin-left:auto;display:flex;gap:6px}
  .tab{margin:0;background:transparent;border:1px solid var(--border);color:var(--muted);padding:5px 14px}
  .tab.active{background:var(--accent);border-color:var(--accent);color:#fff}
  .rs{max-width:1180px;margin:0 auto;padding:20px}
  .lead{color:var(--muted);margin:0 0 14px;max-width:860px}
  .ask{display:flex;gap:10px}.ask input{flex:1;font-size:15px;padding:11px 12px}.ask button{margin:0}
  .examples{display:flex;flex-wrap:wrap;gap:8px;margin:10px 0 18px}
  .chip{margin:0;background:var(--panel2);border:1px solid var(--border);color:var(--fg);font-weight:500;padding:5px 12px;border-radius:999px}
  .rs h3{font-size:13px;text-transform:uppercase;letter-spacing:.6px;color:var(--muted);margin:22px 0 10px;display:flex;gap:8px;align-items:center}
  .agents{display:grid;grid-template-columns:repeat(auto-fill,minmax(340px,1fr));gap:12px}
  .agent{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:14px}
  .agent .q{font-weight:600;margin-bottom:6px}
  .agent .meta{font-size:12px;color:var(--muted);margin-bottom:8px}
  .step{border-left:2px solid var(--border);padding:2px 0 8px 10px;font-size:13px}
  .step .t{color:var(--muted)} .step .c{font-family:var(--mono);font-size:12px;color:var(--accent)}
  .step .r{font-size:12px;color:var(--muted);white-space:pre-wrap;max-height:4.5em;overflow:hidden}
  .resume{color:var(--warn);font-weight:600;font-size:13px;padding:4px 0 8px}
  .drop{color:var(--fail);font-size:12px}
  .final{color:var(--ok)}
  .report{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:16px 18px;line-height:1.7}
  .report p{margin:0 0 10px} .report a{color:var(--accent);text-decoration:none}
  .sources{font-size:13px;color:var(--muted)} .sources a{color:var(--accent)}
  @media(max-width:600px){.ask{flex-direction:column}.agents{grid-template-columns:1fr}}
```

2. 在 `<header>` 末尾（`</header>` 之前）加页签按钮：

```html
  <nav class="tabs"><button class="tab active" data-tab="research">Research</button><button class="tab" data-tab="ops">Ops</button></nav>
```

3. 在 `</header>` 之后、`<main>` 之前插入 Research 视图，并把原 `<main>` 改为 `<main id="tab-ops" hidden>`：

```html
<section id="tab-research" class="rs">
  <p class="lead">Ask a research question. A planner LLM splits it into sub-questions; each one is investigated by a
    ReAct agent (think → call a tool → read the result) running on a Kafka worker pool; a final LLM writes a cited report.
    Kill a worker mid-run (<code>scripts/chaos-agent.sh</code>) and watch another worker resume the agent from its last
    checkpointed step.</p>
  <div class="ask"><input id="q" maxlength="500" placeholder="e.g. What are the trade-offs between Kafka and RabbitMQ for agent systems?"><button id="ask">Research</button></div>
  <div class="examples" id="examples"></div>
  <div id="rsBody" hidden>
    <h3>① Plan <span id="planState" class="badge"></span></h3>
    <ol id="plan"></ol>
    <h3>② Agents</h3>
    <div id="agents" class="agents"></div>
    <h3>③ Report <span id="taskState" class="badge"></span></h3>
    <div id="report" class="report"><span class="muted">Waiting for agents…</span></div>
    <ol id="sources" class="sources"></ol>
    <div id="usageLine" class="hint"></div>
  </div>
</section>
```

4. 在 `<script>` 末尾（`loadUsage();` 之后）追加：

```js
// ---------- 页签 ----------
document.querySelectorAll(".tab").forEach(b=>b.addEventListener("click",()=>{
  document.querySelectorAll(".tab").forEach(x=>x.classList.toggle("active",x===b));
  $("tab-research").hidden=b.dataset.tab!=="research";
  $("tab-ops").hidden=b.dataset.tab!=="ops";
}));

// ---------- Research ----------
// 所有模型/网页文本一律 textContent 渲染;链接只放行 http(s)。
const EXAMPLES=["What are the trade-offs between Kafka and RabbitMQ for agent systems?",
  "How does retrieval-augmented generation reduce LLM hallucinations?",
  "Compare pgvector with dedicated vector databases."];
const TERMINAL=["COMPLETED","FAILED","PARTIAL_FAILED"];
let rsTimer=null;
function el(tag,cls,text){const n=document.createElement(tag);if(cls)n.className=cls;if(text!=null)n.textContent=text;return n;}
function parse(s){try{return JSON.parse(s)}catch{return null}}
function safeLink(url,text){
  if(!/^https?:\/\//i.test(url||""))return el("span",null,text||url);
  const a=el("a",null,text||url);a.href=url;a.target="_blank";a.rel="noopener noreferrer";return a;
}
EXAMPLES.forEach(x=>{const b=el("button","chip",x);b.addEventListener("click",()=>{$("q").value=x;ask();});$("examples").append(b);});
$("ask").addEventListener("click",ask);
$("q").addEventListener("keydown",e=>{if(e.key==="Enter")ask();});

async function ask(){
  const question=$("q").value.trim();
  if(!question)return;
  $("ask").disabled=true;
  try{
    const r=await fetch(`${API}/tasks`,{method:"POST",headers:{"Content-Type":"application/json"},
      body:JSON.stringify({type:"RESEARCH_AGENT",payload:{question}})});
    const d=await r.json();
    if(!r.ok)throw new Error(d.error||r.status);
    watch(d.taskUuid);
  }catch(e){alert("Submit failed: "+e.message);}
  finally{$("ask").disabled=false;}
}

function watch(uuid){
  clearInterval(rsTimer);
  $("rsBody").hidden=false;
  const tick=async()=>{
    try{
      const [t,tr]=await Promise.all([jget(`${API}/tasks/${uuid}`),jget(`${API}/tasks/${uuid}/trace`)]);
      renderResearch(t,tr);
      if(TERMINAL.includes(t.status))clearInterval(rsTimer);
    }catch{/* 轮询中容忍瞬时错误 */}
  };
  tick();rsTimer=setInterval(tick,1000);
}

function renderResearch(t,trace){
  const adv=trace.map(e=>e.stage==="STAGE_ADVANCED"?parse(e.detail):null).find(d=>d&&d.kind==="AGENT");
  const agents=adv?adv.subtasks:[];
  const planFailed=!adv&&TERMINAL.includes(t.status);
  setBadge($("planState"),adv?"COMPLETED":planFailed?"FAILED":"RUNNING",adv?"done":planFailed?"failed":"planning…");
  $("plan").replaceChildren(...agents.map(a=>el("li",null,a.input&&a.input.question)));

  const subBySeq={};(t.subtasks||[]).forEach(s=>subBySeq[s.seq]=s);
  $("agents").replaceChildren(...agents.map(a=>agentCard(a,trace.filter(e=>e.subtaskId===a.subtaskId),subBySeq[a.seq])));

  setBadge($("taskState"),t.status,t.status);
  const items=t.result||[];
  const rep=items.map(i=>i.output).find(o=>o&&o.report);
  if(rep)renderReport(rep);
  else if(TERMINAL.includes(t.status))$("report").replaceChildren(el("span","muted","No report (see agent errors above or the Ops tab)."));

  const u=items.map(i=>i.output&&i.output.usage).filter(Boolean)
    .reduce((s,x)=>({calls:s.calls+x.calls,tokens:s.tokens+x.promptTokens+x.completionTokens,cost:s.cost+x.costMicro}),{calls:0,tokens:0,cost:0});
  $("usageLine").textContent=u.calls?`${u.calls} LLM calls · ${u.tokens.toLocaleString()} tokens · ≈ $${(u.cost/1e6).toFixed(4)}`:"";
}

function setBadge(node,status,text){node.className="badge b-"+status;node.textContent=text;}

function agentCard(a,events,sub){
  const card=el("div","agent");
  card.append(el("div","q",a.input&&a.input.question));
  const workers=[];
  const steps=el("div");
  let lastWorker=null;
  events.forEach(e=>{
    const d=parse(e.detail)||{};
    if(e.stage==="AGENT_STEP"){
      if(d.workerId&&!workers.includes(d.workerId))workers.push(d.workerId);
      lastWorker=d.workerId||lastWorker;
      const s=el("div","step");
      if(d.final){s.append(el("div","final",`#${d.step} ✅ ${d.answer||""}`));}
      else{
        s.append(el("div","t",`#${d.step} 🤔 ${d.thought||"(no reasoning text)"}`));
        (d.calls||[]).forEach(c=>{s.append(el("div","c",`🔧 ${c.tool}(${c.args})`));s.append(el("div","r",`📄 ${c.resultPreview}`));});
      }
      steps.append(s);
    }else if(e.stage==="RESUMED"){
      if(d.workerId&&!workers.includes(d.workerId))workers.push(d.workerId);
      steps.append(el("div","resume",`⚡ ${lastWorker||"previous worker"} died → ${d.workerId} resumed from step ${d.fromStep+1} (tools not re-run)`));
      lastWorker=d.workerId;
    }else if(e.stage==="CITATION_DROPPED"){
      steps.append(el("div","drop",`⚠ dropped unverified source(s): ${(d.urls||[]).join(", ")}`));
    }
  });
  const status=sub?sub.status:"PENDING";
  const meta=el("div","meta");
  const b=el("span","badge b-"+status,status);
  meta.append(b,document.createTextNode(workers.length?`  on ${workers.join(" → ")}`:""));
  if(sub&&sub.error)meta.append(el("div","drop",sub.error));
  card.append(meta,steps);
  return card;
}

function renderReport(rep){
  const box=$("report");box.replaceChildren();
  const max=(rep.sources||[]).length;
  String(rep.report).split(/\n{2,}/).forEach(par=>{
    const p=el("p");
    par.split(/(\[\d{1,3}\])/).forEach(part=>{
      const m=part.match(/^\[(\d{1,3})\]$/);
      if(m&&+m[1]>=1&&+m[1]<=max){const a=el("a",null,part);a.href="#src-"+m[1];p.append(a);}
      else p.append(document.createTextNode(part));
    });
    box.append(p);
  });
  $("sources").replaceChildren(...(rep.sources||[]).map(s=>{
    const li=el("li");li.id="src-"+s.n;li.append(safeLink(s.url,s.title));
    if(/^https?:/i.test(s.url||""))li.append(document.createTextNode(" — "+s.url));
    return li;
  }));
}
```

- [ ] **Step 3: 本地跑起来，人工检查**

```bash
mvn -B -q install -DskipTests                                                              # 先把 common 装进本地仓库
mvn -B -q -f agentflow-server/pom.xml spring-boot:run                                       # 终端 A(:8080)
mvn -B -q -f agentflow-worker/pom.xml spring-boot:run -Dspring-boot.run.arguments="--llm.mock.delay-ms=1500"   # 终端 B
```

打开 `http://localhost:8080/`，点第一个示例问题。Expected：Plan 区出现 3 个子问题；每个 agent 卡片依次出现 🤔/🔧/📄 步骤，最后出现 ✅；Report 区出现 mock 报告；页脚显示调用次数和 token 数。切到 Ops 页签，原有视图正常。用 chrome-devtools MCP 截图存证。

- [ ] **Step 4: 用 devtools 验证 XSS 防护（Review Focus 1、2）**

在页面上执行（chrome-devtools `evaluate_script`）：

```js
() => {
  renderTrace([{stage:"AGENT_STEP",status:null,subtaskId:1,timestamp:0,detail:'<img src=x onerror="window.__xss=1">'}]);
  renderResearch({status:"COMPLETED",subtasks:[{seq:1,status:"COMPLETED"}],
    result:[{output:{report:'<img src=x onerror="window.__xss=2"> see [1] and [9]',
      sources:[{n:1,title:"evil",url:"javascript:window.__xss=3"}],usage:{calls:1,promptTokens:1,completionTokens:1,costMicro:1}}}]},
    [{stage:"STAGE_ADVANCED",detail:JSON.stringify({kind:"AGENT",subtasks:[{subtaskId:1,seq:1,input:{question:"<b>q</b>"}}]})},
     {stage:"AGENT_STEP",subtaskId:1,detail:JSON.stringify({step:1,thought:"<script>window.__xss=4</script>",calls:[],workerId:"w"})}]);
  return {xss: window.__xss ?? null, imgs: document.querySelectorAll("img").length,
          jsLinks: document.querySelectorAll('a[href^="javascript"]').length,
          citeLinks: [...document.querySelectorAll('#report a')].map(a=>a.textContent)};
}
```

Expected：`{xss: null, imgs: 0, jsLinks: 0, citeLinks: ["[1]"]}`（`[9]` 超出来源范围，不会变成链接）。

- [ ] **Step 5: commit**

```bash
git add agentflow-server/src/main/resources/static/index.html
git commit -m "feat(console): Research 页签实时展示拆题/agent 每一步/续跑/带引用报告;Ops 页签修 XSS"
```

---

### Task 9: 容器化，一条命令启动，e2e 与 chaos 脚本

**Files:**
- Modify: `pom.xml`（spring-boot-maven-plugin 绑定 repackage）
- Create: `docker/java.Dockerfile`、`.dockerignore`、`agentflow-rag/Dockerfile`、`agentflow-rag/.dockerignore`
- Modify: `docker-compose.yml`
- Create: `.env.example`；Modify: `.gitignore`
- Create: `scripts/e2e-agent.sh`、`scripts/chaos-agent.sh`

- [ ] **Step 1: 让 jar 可以直接 `java -jar` 运行**

父 POM 用的是 BOM import，没有继承 `spring-boot-starter-parent`，所以 `repackage` 没有自动绑定。在 `pom.xml` 的 `pluginManagement` 中，给 spring-boot-maven-plugin 加上：

```xml
          <executions>
            <execution>
              <goals><goal>repackage</goal></goals>
            </execution>
          </executions>
```

Run: `mvn -B -q -DskipTests package && unzip -p agentflow-worker/target/agentflow-worker-0.1.0-SNAPSHOT.jar META-INF/MANIFEST.MF | grep Main-Class`
Expected: 输出 `Main-Class: org.springframework.boot.loader.launch.JarLauncher`。

- [ ] **Step 2: Dockerfile**

`docker/java.Dockerfile`：

```dockerfile
# 通用 Java 镜像:构建参数 MODULE=agentflow-server | agentflow-worker
FROM maven:3.9-eclipse-temurin-17 AS build
ARG MODULE
WORKDIR /src
COPY pom.xml .
COPY agentflow-common/pom.xml agentflow-common/
COPY agentflow-server/pom.xml agentflow-server/
COPY agentflow-worker/pom.xml agentflow-worker/
# 先只拷 pom 拉依赖:源码变动时这一层走缓存
RUN mvn -B -q -pl ${MODULE} -am dependency:go-offline || true
COPY agentflow-common agentflow-common
COPY agentflow-server agentflow-server
COPY agentflow-worker agentflow-worker
RUN mvn -B -q -pl ${MODULE} -am package -DskipTests

FROM eclipse-temurin:17-jre
ARG MODULE
WORKDIR /app
COPY --from=build /src/${MODULE}/target/${MODULE}-*.jar app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
```

`.dockerignore`：

```
**/target
.git
.env
agentflow-rag/.venv
**/__pycache__
```

`agentflow-rag/Dockerfile`：

```dockerfile
FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app app
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
```

`agentflow-rag/.dockerignore`：

```
.venv
__pycache__
tests
```

- [ ] **Step 3: docker-compose.yml 补全**

改动如下（`mysql` 不变）：

1. `redis` 加 `healthcheck: {test: ["CMD", "redis-cli", "ping"], interval: 5s, retries: 20}`。
2. `mongo` 加 `healthcheck: {test: ["CMD", "mongosh", "--quiet", "--eval", "db.adminCommand('ping')"], interval: 5s, retries: 20}`。
3. `kafka` 改为双 listener（容器内用 `kafka:19092`，宿主机仍是 `localhost:9092`，本地开发和测试不受影响），并加健康检查：

```yaml
      KAFKA_LISTENERS: PLAINTEXT://:19092,EXTERNAL://:9092,CONTROLLER://:9093
      # 容器内(server/worker)连 kafka:19092;宿主机(本地开发、测试)连 localhost:9092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:19092,EXTERNAL://localhost:9092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
    healthcheck:
      test: ["CMD-SHELL", "/opt/kafka/bin/kafka-broker-api-versions.sh --bootstrap-server localhost:9092 >/dev/null 2>&1"]
      interval: 10s
      retries: 30
```

4. 新增服务：

```yaml
  pgvector:
    image: pgvector/pgvector:pg16
    container_name: agentflow-pgvector
    ports: ["5433:5432"]
    environment:
      POSTGRES_PASSWORD: agentflow
      POSTGRES_DB: agentflow_rag
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "postgres"]
      interval: 5s
      retries: 20

  rag:
    build: ./agentflow-rag
    container_name: agentflow-rag
    environment:
      RAG_DB_URL: postgresql://postgres:agentflow@pgvector:5432/agentflow_rag
    ports: ["${RAG_PORT:-8000}:8000"]
    depends_on:
      pgvector: {condition: service_healthy}
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/health')"]
      interval: 5s
      retries: 20

  server:
    build: {context: ., dockerfile: docker/java.Dockerfile, args: {MODULE: agentflow-server}}
    container_name: agentflow-server
    ports: ["8080:8080"]
    environment:
      SPRING_DATASOURCE_URL: jdbc:mysql://mysql:3306/agentflow?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:19092
      SPRING_DATA_REDIS_HOST: redis
      SPRING_DATA_MONGODB_URI: mongodb://mongo:27017/agentflow_trace
    depends_on:
      mysql: {condition: service_healthy}
      redis: {condition: service_healthy}
      mongo: {condition: service_healthy}
      kafka: {condition: service_healthy}

  worker-1: &worker
    build: {context: ., dockerfile: docker/java.Dockerfile, args: {MODULE: agentflow-worker}}
    container_name: agentflow-worker-1
    hostname: worker-1          # 页面上显示的 workerId;chaos 脚本据此 docker kill
    environment:
      SPRING_KAFKA_BOOTSTRAP_SERVERS: kafka:19092
      SPRING_DATA_REDIS_HOST: redis
      RAG_BASEURL: http://rag:8000
      LLM_PROVIDER: ${LLM_PROVIDER:-mock}
      LLM_DEFAULTMODEL: ${LLM_MODEL:-mock-small}
      LLM_MODEL: ${LLM_MODEL:-mock-small}
      LLM_PRICE_PER1K: ${LLM_PRICE_PER1K:-0.001}
      LLM_MOCK_DELAYMS: ${LLM_MOCK_DELAY_MS:-1500}
      OPENAI_BASE_URL: ${OPENAI_BASE_URL:-https://api.openai.com/v1}
      OPENAI_API_KEY: ${OPENAI_API_KEY:-}
    depends_on:
      redis: {condition: service_healthy}
      kafka: {condition: service_healthy}
      rag: {condition: service_healthy}
      server: {condition: service_started}

  worker-2:
    <<: *worker
    container_name: agentflow-worker-2
    hostname: worker-2
```

- [ ] **Step 4: `.env.example` 与 `.gitignore`**

`.env.example`：

```bash
# 复制为 .env 后按需修改。不填 key 也能跑:默认 mock(固定剧本、不花钱)。
LLM_PROVIDER=mock
LLM_MODEL=mock-small
OPENAI_BASE_URL=
OPENAI_API_KEY=
LLM_PRICE_PER1K=0.001
# 真实模型示例(任一 OpenAI 兼容服务),以 DeepSeek 为例:
#   LLM_PROVIDER=openai
#   LLM_MODEL=deepseek-chat
#   OPENAI_BASE_URL=https://api.deepseek.com/v1
#   OPENAI_API_KEY=sk-...
# mock 模式每次调用的模拟耗时(毫秒),便于在页面看清每一步、演示杀 worker
LLM_MOCK_DELAY_MS=1500
# RAG 服务的宿主机端口(8000 被占用时改掉)
RAG_PORT=8000
```

`.gitignore` 追加一行 `.env`（先用 `cat .gitignore` 看一下，已有就不加）。

- [ ] **Step 5: e2e 与 chaos 脚本**

`scripts/e2e-agent.sh`：

```bash
#!/usr/bin/env bash
# RESEARCH_AGENT 端到端:提交问题 → 轮询到终态 → 断言生成了报告。前置:docker compose up -d
set -euo pipefail
BASE="${BASE:-http://localhost:8080/api/v1/tasks}"
Q="${1:-What are the trade-offs between Kafka and RabbitMQ for agent systems?}"
# 等 server 就绪(compose --wait 只保证容器已启动,Spring 可能还在初始化)
for i in $(seq 1 60); do curl -sf "${BASE%/tasks}/llm/usage" >/dev/null && break; sleep 2; done

BODY=$(python3 -c 'import json,sys; print(json.dumps({"type":"RESEARCH_AGENT","payload":{"question":sys.argv[1]}}))' "$Q")
UUID=$(curl -sf -X POST "$BASE" -H 'Content-Type: application/json' -d "$BODY" \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["taskUuid"])')
echo "==> taskUuid=$UUID"

for i in $(seq 1 90); do
  TASK=$(curl -sf "$BASE/$UUID")
  STATUS=$(echo "$TASK" | python3 -c 'import sys,json; print(json.load(sys.stdin)["status"])')
  echo "    [$i] status=$STATUS"
  case "$STATUS" in
    COMPLETED|PARTIAL_FAILED)
      echo "$TASK" | python3 -c '
import sys,json
t=json.load(sys.stdin)
rep=[i["output"] for i in t["result"] if isinstance(i.get("output"),dict) and "report" in i["output"]]
assert rep, "没有生成报告"
print(rep[0]["report"]); print("来源数:", len(rep[0]["sources"]))
'
      echo "✅ E2E-AGENT PASS ($STATUS)"; exit 0;;
    FAILED) echo "$TASK" | python3 -m json.tool; echo "❌ 任务失败"; exit 1;;
  esac
  sleep 2
done
echo "❌ 180s 超时"; exit 1
```

`scripts/chaos-agent.sh`：

```bash
#!/usr/bin/env bash
# 容错演示:agent 跑到一半时杀掉它所在的 worker → 另一个 worker 从存档续跑,已调用的工具不重跑。
# 前置:docker compose up -d(两个 worker;mock 模式默认每次调用 1.5s,来得及杀)
set -euo pipefail
BASE="${BASE:-http://localhost:8080/api/v1/tasks}"
Q="${1:-How does Kafka consumer group rebalancing work?}"
# 等 server 就绪(compose --wait 只保证容器已启动,Spring 可能还在初始化)
for i in $(seq 1 60); do curl -sf "${BASE%/tasks}/llm/usage" >/dev/null && break; sleep 2; done

BODY=$(python3 -c 'import json,sys; print(json.dumps({"type":"RESEARCH_AGENT","payload":{"question":sys.argv[1]}}))' "$Q")
UUID=$(curl -sf -X POST "$BASE" -H 'Content-Type: application/json' -d "$BODY" \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["taskUuid"])')
echo "==> taskUuid=$UUID,等待第一个 agent 步骤…"

VICTIM=""
for i in $(seq 1 60); do
  VICTIM=$(curl -sf "$BASE/$UUID/trace" | python3 -c '
import sys,json
for e in json.load(sys.stdin):
    if e["stage"]=="AGENT_STEP":
        d=json.loads(e["detail"])
        if not d.get("final"):
            print(d["workerId"]); break
')
  [ -n "$VICTIM" ] && break
  sleep 1
done
[ -n "$VICTIM" ] || { echo "❌ 60s 内没有出现 agent 步骤"; exit 1; }

echo "==> docker kill agentflow-$VICTIM(它正在跑 agent 的下一步)"
docker kill "agentflow-$VICTIM" >/dev/null
trap 'docker start "agentflow-$VICTIM" >/dev/null' EXIT

echo "==> 等待其他 worker 接手(Kafka 会话超时约 45s)…"
STATUS=RUNNING
for i in $(seq 1 120); do
  STATUS=$(curl -sf "$BASE/$UUID" | python3 -c 'import sys,json; print(json.load(sys.stdin)["status"])')
  case "$STATUS" in COMPLETED|PARTIAL_FAILED|FAILED) break;; esac
  sleep 2
done

curl -sf "$BASE/$UUID/trace" | python3 -c '
import sys,json
res=[json.loads(e["detail"]) for e in json.load(sys.stdin) if e["stage"]=="RESUMED"]
assert res, "没有出现 RESUMED:agent 没有从存档续跑"
for r in res:
    print("  ⚡", r["workerId"], "从第", r["fromStep"] + 1, "步续跑")
'
echo "==> 任务终态:$STATUS"
if [ "$STATUS" = "FAILED" ]; then echo "❌ 任务失败"; exit 1; fi
echo "✅ CHAOS PASS"
```

执行 `chmod +x scripts/e2e-agent.sh scripts/chaos-agent.sh`。

- [ ] **Step 6: 一条命令启动并验证**

先停掉 Task 8 里用 `spring-boot:run` 启动的本地进程，再执行：

```bash
cp -n .env.example .env
docker compose up -d --build --wait
scripts/e2e-agent.sh
scripts/chaos-agent.sh
scripts/e2e-smoke.sh            # 回归:旧的 ECHO_BATCH 主链路
```

Expected：三个脚本依次输出 `✅ E2E-AGENT PASS`、`✅ CHAOS PASS`（中间打印一行「⚡ worker-x 从第 N 步续跑」）、`✅ E2E PASS`。

如果 chaos 失败，先排查：`docker logs agentflow-worker-2 | grep 续跑`，确认 offset 没有在处理完成前就被提交（`spring.kafka.listener.ack-mode` 默认是 BATCH，处理完一条才提交）。

- [ ] **Step 7: commit**

```bash
git add pom.xml docker docker-compose.yml .dockerignore agentflow-rag/Dockerfile agentflow-rag/.dockerignore \
        .env.example .gitignore scripts/e2e-agent.sh scripts/chaos-agent.sh
git commit -m "feat(deploy): 一条命令启动全栈(9 服务 + 健康检查)+ e2e/chaos 脚本"
```

---

### Task 10: GitHub Actions CI

**Files:**
- Create: `.github/workflows/ci.yml`

- [ ] **Step 1: 写 workflow**

```yaml
name: CI
on:
  push:
  pull_request:

jobs:
  java:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with:
          distribution: temurin
          java-version: '17'
          cache: maven
      - name: Start middleware
        run: docker compose up -d --wait mysql redis mongo kafka pgvector rag
      - name: Test (mock LLM, no API key)
        run: mvn -B test

  rag:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - name: Start pgvector
        run: docker compose up -d --wait pgvector
      - uses: actions/setup-python@v5
        with:
          python-version: '3.12'
      - run: pip install -r agentflow-rag/requirements.txt
      - run: cd agentflow-rag && pytest -q
```

- [ ] **Step 2: 本地模拟 CI 环境验证**

```bash
docker compose down -v                                   # 用全新的库,模拟 CI(init.sql 已含 kind 列)
docker compose up -d --wait mysql redis mongo kafka pgvector rag
mvn -B test 2>&1 | tail -15
cd agentflow-rag && { [ -d .venv ] || (python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt); } \
  && .venv/bin/pytest -q 2>&1 | tail -5; cd ..
```

Expected：Java 和 RAG 测试全部 PASS。注意 `down -v` 会清空本地 compose 的数据卷，只影响本机 Mac 上的开发数据，不涉及家里那台机器。

- [ ] **Step 3: commit**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: GitHub Actions(Java 全量测试 + RAG pytest,全程 mock 无需 key)"
```

---

### Task 11: README（英文默认，中文版互链）

**Files:**
- Rename: `README.md` → `README.zh.md`；`README.en.md` → `README.md`
- Modify: `README.md`、`README.zh.md`

- [ ] **Step 1: 改名**

```bash
git mv README.md README.zh.md
git mv README.en.md README.md
```

- [ ] **Step 2: 改写英文 README 首屏**

把 `README.md` 从开头到 `## Architecture` 之前的内容替换为下面这段（README 后面原有的章节保留，只做 Step 3 中的修改）：

````markdown
# AgentFlow

[![CI](https://github.com/congfanpu-lgtm/agentflow/actions/workflows/ci.yml/badge.svg)](https://github.com/congfanpu-lgtm/agentflow/actions/workflows/ci.yml)
· [中文](README.zh.md)

**A crash-tolerant multi-agent research system on a Kafka worker pool.** Ask a question → a planner LLM splits it
into sub-questions → each sub-question is researched in parallel by a ReAct agent (native tool calling:
Wikipedia, web fetch, local RAG) → a final LLM writes a report with verified citations.
Kill a worker mid-run and the agent resumes on another worker **from its last checkpointed step, without
re-running tools**.

![demo](docs/img/demo.gif)

```bash
cp .env.example .env          # default: mock LLM (no key, no cost). Set LLM_PROVIDER=openai + key for a real model
docker compose up -d --build --wait
open http://localhost:8080    # click an example question
scripts/chaos-agent.sh        # kill a worker mid-run, watch the agent resume
```

```mermaid
flowchart LR
  UI[Console] -->|RESEARCH_AGENT| S[Server<br/>CAS state machine]
  S -->|PLAN| K[(Kafka)]
  K --> W1[Worker 1] & W2[Worker 2]
  W1 & W2 -->|result| S
  S -->|N × AGENT| K
  S -->|SYNTHESIZE| K
  W1 & W2 <-->|checkpoint per step| R[(Redis)]
  W1 & W2 -->|LLM via gateway<br/>rate limit + token accounting| L[LLM API]
  W1 & W2 -->|trace| T[(MongoDB)]
```

**How a research task runs**

| Stage | Subtasks | What happens |
|---|---|---|
| ① Plan | 1 | LLM splits the question into 2–4 sub-questions (schema-validated) |
| ② Agents | N, in parallel | ReAct loop, max 6 steps; each step checkpointed to Redis; tools are read-only |
| ③ Synthesize | 1 | LLM writes the report; citations limited to URLs tools actually returned |

Every stage is an ordinary Kafka subtask, so it inherits the existing reliability layer: staged retries
(5s/30s/5m) → DLQ with replay, timeout sweep, idempotency guard, partial-failure semantics, full run trace.

> ⚠️ No authentication. Keep it on localhost / a private network; add auth before exposing it publicly,
> especially with a real LLM key configured.
````

- [ ] **Step 3: 更新英文 README 的其余章节**

1. 在 `## Self-built core (résumé highlights)` 的列表开头加 3 条：

```markdown
- **Resumable ReAct agents** — native OpenAI tool calling; per-step checkpoint in Redis keyed by subtask UUID;
  on redelivery (worker crash, retry, timeout sweep) the agent replays saved steps and continues — tools are never re-run.
  At-least-once delivery + read-only tools ⇒ effectively exactly-once tool side effects.
- **Multi-stage orchestration** — plan → map → reduce built on the same subtask state machine; the next stage is
  created in the same DB transaction that settles the last result (row lock on the task counter ⇒ created exactly once),
  dispatched after commit.
- **Agent guardrails** — SSRF guard on `web_fetch` (blocks loopback/private/link-local/CGNAT, re-checked on every redirect);
  tool errors fed back to the model instead of crashing; duplicate-call cache; fabricated citations dropped and traced.
```

2. 把 `## Quick start` 整节替换为「`docker compose up -d --build --wait`」+「本地开发：`docker compose up -d mysql redis mongo kafka pgvector rag`，再用 `mvn spring-boot:run` 启动 server 和 worker」，并补一句已有部署的升级方式：`ALTER` 见 `docker/mysql/migrations/2026-10-15-subtask-kind.sql`。
3. 在 `## Roadmap` 中加一条 Future work：`Durable execution with one Kafka message per agent step (Temporal-style) — today a long loop occupies one worker; SSE instead of 1s polling; general web search tool; auth.`
4. 删掉 Quick start 里单独 `docker run pgvector` 的那条命令（pgvector 已经进了 compose）。

- [ ] **Step 4: 更新中文 README**

在 `README.zh.md` 第一行标题下方加：`[English](README.md) · 本文为中文版`。新增一节 `## 研究 Agent(RESEARCH_AGENT)`，内容是 Step 2 中三阶段表格与 3 条卖点的中文版。同时把「pgvector 未进 compose / docker run」相关段落改为「已合入 docker-compose.yml」。

- [ ] **Step 5: demo GIF 占位说明**

`docs/img/demo.gif` 由作者在本机录制（这一步不能自动化）。在 `README.zh.md` 末尾加「录制 demo」小节：

```markdown
## 录制 demo GIF
1. `docker compose up -d --build --wait`,浏览器打开 http://localhost:8080 并缩放到 1280×800;
2. 用 Kap / LICEcap 开始录屏,点第一个示例问题;
3. 等第一个 agent 出现 🔧 后,在终端执行 `scripts/chaos-agent.sh`(或 `docker kill agentflow-worker-1`);
4. 录到页面出现「⚡ … resumed from step N」与最终报告,保存为 `docs/img/demo.gif`(< 8MB)。
```

在 GIF 录好之前，README 中 `![demo](docs/img/demo.gif)` 这一行会显示为破图。所以 GIF 录好之前先把这一行注释掉：`<!-- ![demo](docs/img/demo.gif) -->`。

- [ ] **Step 6: commit**

```bash
git add README.md README.zh.md
git commit -m "docs: README 改为英文默认(首屏:一句话 + 一条命令 + 架构图 + CI 徽章),中文版互链"
```

---

### Task 12: 面试讲解文档与简历 bullets

**Files:**
- Create: `docs/interview-walkthrough.md`
- Modify: `docs/resume-bullets.md`

这是文档任务，内容以最终代码为准，引用的文件和行号必须先用 grep 核实过。

- [ ] **Step 1: 写 `docs/interview-walkthrough.md`（中文）**

结构必须包含：

1. **30 秒版**与 **2 分钟版**口述稿，覆盖：做了什么、为什么不用现成框架、最难的点、量化结果（只写实测过的数字）。
2. **一个请求的旅程**：按以下顺序，每站写「代码在哪（`文件:行`）/ 做了什么 / 为什么这样设计 / 面试官可能追问的 2~3 个问题 + 回答要点」：
   `TaskController` → `ResearchAgentDecomposer.decompose` → `SubtaskDispatcher` → `SubtaskListener` → `PlanProcessor` → `ResultHandleService` → `StageAdvancer`（重点讲并发：行锁让下一阶段只创建一次；afterCommit 分发） → `AgentProcessor` / `AgentLoop`（重点讲存档时机、续跑、at-least-once + 只读工具） → 三个工具与 `UrlGuard` → `SynthesizeProcessor` → `TaskFinalizer` → 页面。
3. **必答题**（每题给出回答要点）：
   - 为什么 worker 不连 MySQL？
   - 存档为什么放 Redis？Redis 挂了或数据丢了会怎样？
   - 崩溃发生在「工具执行完、存档之前」会怎样？算不算 exactly-once？
   - 两个 worker 同时跑同一个子任务会怎样？（超时重投 + 原 worker 还活着；load 按步号去重；严格互斥要加锁）
   - 为什么不用 LangChain4j / Temporal？方案 B（每步一条消息）和现在的方案比较。
   - 怎么防止模型编造引用？怎么防止 SSRF 和提示注入让 agent 访问内网？
   - 上下文怎么控制？为什么不能像 `AgentContext` 那样逐条丢历史？
   - 部分失败的语义：一个 agent 失败，报告还生成吗？任务是什么状态？
   - 限流和成本：所有 LLM 调用为什么都要经过网关？
4. **已知局限与下一步**：对应代码中所有 `ponytail:` 注释（`grep -rn "ponytail:" agentflow-*/src/main`），逐条列出。

- [ ] **Step 2: 更新 `docs/resume-bullets.md`**

追加「RESEARCH_AGENT（2026-10）」小节，给出中英文各 2~3 条 bullet。只写代码里真实存在的能力；数字只用实测值（e2e/chaos 脚本的输出、测试数）。没有实测的数字一律不写。

- [ ] **Step 3: commit**

```bash
git add docs/interview-walkthrough.md docs/resume-bullets.md
git commit -m "docs: 面试讲解文档(请求旅程 + 必答题)与简历 bullets"
```

---

### Task 13（需要用户确认后才执行）：部署到家里机器

**先问用户。** 这一步会删除家里机器上手动启动的 `agentflow-server`、`agentflow-worker`、`agentflow-rag`、`agentflow-pgvector` 容器（pgvector 里的知识库数据会丢失，需要重新 ingest），并对 MySQL 执行 `ALTER`。

- [ ] **Step 1: 同步代码（不 push 到 GitHub）**

```bash
rsync -az --exclude target --exclude .git --exclude .venv --exclude .env \
  /Users/cong/Documents/project/project4find-job/agentflow/ Upton@100.65.10.56:~/project/agentflow/
```

- [ ] **Step 2: 远程执行迁移与启动**

```bash
ssh Upton@100.65.10.56 'cd ~/project/agentflow && \
  docker exec -i agentflow-mysql mysql -uroot -pagentflow123 agentflow < docker/mysql/migrations/2026-10-15-subtask-kind.sql; \
  docker rm -f agentflow-server agentflow-worker agentflow-rag agentflow-pgvector; \
  cp -n .env.example .env && sed -i "s/^RAG_PORT=.*/RAG_PORT=8001/" .env && \
  docker compose up -d --build --wait && scripts/e2e-agent.sh'
```

Expected：`✅ E2E-AGENT PASS`。家里机器的 8000 端口被 financial-freedom-tracker 占用，所以 RAG 映射到 8001。如果要切换到真实模型，由用户在家里机器的 `.env` 中填写 key，**key 不要经过本次会话**。

---

## 执行后的汇报清单

- 每个任务的测试结果（附命令输出摘要）；基线中原本就失败的测试单独列出。
- 页面截图（Task 8 Step 3）与 devtools XSS 校验的返回值（Task 8 Step 4）。
- `e2e-agent.sh`、`chaos-agent.sh`、`e2e-smoke.sh` 的输出。
- 需要用户做的事：录制 GIF；决定是否 push（push 后 CI 徽章才会生效）；决定是否执行 Task 13；配置真实模型 key；读 `docs/interview-walkthrough.md`；同步中英文简历。
