package com.agentflow.worker.llm;

import com.agentflow.common.llm.ChatMessage;
import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import com.agentflow.common.llm.ToolSpec;
import com.agentflow.worker.agent.Prompts;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
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
        assertEquals("{\"query\":\"kafka\"}", s1.getToolCalls().get(0).getArgumentsJson());

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

    @Test
    void agentScriptSearchesKeywordsNotWholeSentence() {
        assertEquals("Kafka RabbitMQ", MockLlmClient.keywords(
                "What are the trade-offs between Kafka and RabbitMQ for agent systems? — 关键取舍"));
        assertEquals("retrieval-augmented generation reduce", MockLlmClient.keywords(
                "how does retrieval-augmented generation reduce hallucinations"));
    }
}
