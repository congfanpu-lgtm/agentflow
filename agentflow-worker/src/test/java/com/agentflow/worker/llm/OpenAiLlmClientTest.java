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
        server.createContext("/err/chat/completions", ex -> {
            ex.getRequestBody().readAllBytes();
            byte[] resp = ("{\"error\":{\"message\":\"Incorrect API key provided: sk-proj-SECRET123456. "
                    + "x".repeat(1000) + "\"}}").getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(401, resp.length);
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

    @Test
    void mergesExtraBodyIntoRequest() throws Exception {
        // Anthropic 兼容接口默认会思考,隐藏的思考吃掉 max_tokens → 答案被截断;靠 extra-body 关掉
        OpenAiLlmClient c = client();
        ReflectionTestUtils.setField(c, "extraBody", "{\"thinking\":{\"type\":\"disabled\"}}");
        c.chat(new ChatRequest("m", List.of(new ChatMessage("user", "hi")), 10));
        JsonNode sent = new ObjectMapper().readTree(sentBody.get());
        assertEquals("disabled", sent.at("/thinking/type").asText());
        assertEquals("m", sent.path("model").asText());
    }

    @Test
    void errorKeepsStatusButNeverProviderBody() {
        // 错误信息会进 trace / 子任务 errorMsg,控制台无鉴权可见;服务商响应体可能回显 key,一律不进异常链
        OpenAiLlmClient c = client();
        ReflectionTestUtils.setField(c, "baseUrl", "http://127.0.0.1:" + server.getAddress().getPort() + "/err");
        RuntimeException e = assertThrows(RuntimeException.class,
                () -> c.chat(new ChatRequest("m", List.of(new ChatMessage("user", "hi")), 10)));
        assertTrue(e.getMessage().contains("HTTP 401"), e.getMessage());
        for (Throwable t = e; t != null; t = t.getCause()) {
            assertFalse(String.valueOf(t.getMessage()).contains("SECRET123456"), t.toString());
        }
    }

    @Test
    void redactMasksKeysAndTruncates() {
        String out = OpenAiLlmClient.redact("bad key sk-proj-SECRET123456 and tvly-dev-ABCDEF99 " + "x".repeat(1000));
        assertFalse(out.contains("SECRET123456") || out.contains("ABCDEF99"), out);
        assertTrue(out.contains("sk-***"), out);
        assertTrue(out.length() <= 320, "应截断:" + out.length());
    }

    @Test
    void usesConfiguredMaxTokensParam() throws Exception {
        // 新 OpenAI 模型拒绝 max_tokens,只认 max_completion_tokens;其他兼容服务仍用 max_tokens(默认)
        OpenAiLlmClient c = client();
        ReflectionTestUtils.setField(c, "maxTokensParam", "max_completion_tokens");
        c.chat(new ChatRequest("m", List.of(new ChatMessage("user", "hi")), 10));
        JsonNode sent = new ObjectMapper().readTree(sentBody.get());
        assertEquals(10, sent.path("max_completion_tokens").asInt());
        assertFalse(sent.has("max_tokens"));
    }
}
