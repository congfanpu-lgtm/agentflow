package com.agentflow.worker.llm;

import com.agentflow.common.llm.ChatMessage;
import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** MockLlmClient 单测:确定性输出 + token 估算 > 0。无需任何中间件。 */
class MockLlmClientTest {

    private final MockLlmClient client = new MockLlmClient();

    private ChatRequest req(String user) {
        return new ChatRequest("mock-small",
                List.of(new ChatMessage("system", "你是摘要助手"),
                        new ChatMessage("user", user)), 128);
    }

    @Test
    void deterministicSchemaConformingJson() {
        ChatResponse a = client.chat(req("请总结这段文字"));
        ChatResponse b = client.chat(req("请总结这段文字"));
        assertEquals(a.getContent(), b.getContent());
        assertTrue(a.getContent().contains("\"summary\""), "应输出含 summary 的 JSON");
        assertTrue(a.getContent().trim().startsWith("{"), "应是 JSON 对象");
        assertEquals("mock-small", a.getModel());
    }

    @Test
    void reportsPositiveTokenUsage() {
        ChatResponse r = client.chat(req("hello world this is a test input"));
        assertTrue(r.getUsage().getPromptTokens() > 0);
        assertTrue(r.getUsage().getCompletionTokens() > 0);
        assertEquals("mock", client.provider());
    }
}
