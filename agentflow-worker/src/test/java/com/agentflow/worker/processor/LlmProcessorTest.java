package com.agentflow.worker.processor;

import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import com.agentflow.common.llm.TokenUsage;
import com.agentflow.worker.context.AgentContext;
import com.agentflow.worker.context.TokenEstimator;
import com.agentflow.worker.llm.LlmGateway;
import com.agentflow.worker.llm.LlmModelProperties;
import com.agentflow.worker.skill.SchemaValidator;
import com.agentflow.worker.skill.SkillRegistry;
import com.agentflow.worker.skill.SummarizeSkill;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** LlmProcessor 单测:mock 网关。验证 schema 合规通过 + 不合规抛(交重试)+ 经 AgentContext 组 prompt。 */
class LlmProcessorTest {

    private final ObjectMapper om = new ObjectMapper();
    private final LlmGateway gateway = mock(LlmGateway.class);

    private LlmProcessor processor() {
        LlmModelProperties props = new LlmModelProperties();
        AgentContext ctx = new AgentContext(new TokenEstimator(), props);
        SkillRegistry registry = new SkillRegistry(List.of(new SummarizeSkill()));
        return new LlmProcessor(gateway, ctx, registry, new SchemaValidator());
    }

    @Test
    void conformingOutputPassesAndIsAugmented() throws Exception {
        when(gateway.chat(any())).thenReturn(
                new ChatResponse("{\"summary\":\"ok\"}", new TokenUsage(5, 7), "mock-small"));
        String out = processor().process("{\"text\":\"some long document to summarize\"}");
        JsonNode node = om.readTree(out);
        assertEquals("ok", node.get("summary").asText());
        assertEquals("summarize", node.get("skill").asText());
        assertEquals("mock-small", node.get("model").asText());
        assertEquals(5, node.get("promptTokens").asInt());
        assertEquals(7, node.get("completionTokens").asInt());
    }

    @Test
    void nonConformingOutputThrows() {
        when(gateway.chat(any())).thenReturn(
                new ChatResponse("{\"wrong\":1}", new TokenUsage(1, 1), "mock-small"));
        assertThrows(IllegalStateException.class,
                () -> processor().process("{\"text\":\"doc\"}"));
    }

    @Test
    void nonJsonOutputThrows() {
        when(gateway.chat(any())).thenReturn(
                new ChatResponse("free-form model rambling", new TokenUsage(1, 1), "mock-small"));
        assertThrows(IllegalStateException.class,
                () -> processor().process("{\"text\":\"doc\"}"));
    }

    @Test
    void buildsPromptViaAgentContextWithSystemAndUser() throws Exception {
        when(gateway.chat(any())).thenReturn(
                new ChatResponse("{\"summary\":\"s\"}", new TokenUsage(1, 1), "mock-small"));
        processor().process("{\"text\":\"hello\"}");
        ArgumentCaptor<ChatRequest> cap = ArgumentCaptor.forClass(ChatRequest.class);
        verify(gateway).chat(cap.capture());
        var msgs = cap.getValue().getMessages();
        assertEquals("system", msgs.get(0).getRole());
        assertEquals("user", msgs.get(msgs.size() - 1).getRole());
        assertEquals("hello", msgs.get(msgs.size() - 1).getContent());
    }
}
