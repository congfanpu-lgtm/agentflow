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
