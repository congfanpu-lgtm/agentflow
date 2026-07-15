package com.agentflow.worker.processor;

import com.agentflow.common.llm.ChatResponse;
import com.agentflow.common.llm.TokenUsage;
import com.agentflow.worker.context.AgentContext;
import com.agentflow.worker.context.TokenEstimator;
import com.agentflow.worker.llm.LlmGateway;
import com.agentflow.worker.llm.LlmModelProperties;
import com.agentflow.worker.rag.Hit;
import com.agentflow.worker.rag.RagClient;
import com.agentflow.worker.rag.RagProperties;
import com.agentflow.worker.skill.RagSearchSkill;
import com.agentflow.worker.skill.SchemaValidator;
import com.agentflow.worker.skill.SkillRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** ResearchProcessor 单测:意图门控——命中→调 RAG(usedRag=true);不命中→不调 RAG(usedRag=false)。 */
class ResearchProcessorTest {

    private final ObjectMapper om = new ObjectMapper();
    private final RagClient ragClient = mock(RagClient.class);
    private final LlmGateway gateway = mock(LlmGateway.class);

    private ResearchProcessor processor() {
        LlmModelProperties props = new LlmModelProperties();
        AgentContext ctx = new AgentContext(new TokenEstimator(), props);
        SkillRegistry registry = new SkillRegistry(List.of(new RagSearchSkill()));
        return new ResearchProcessor(ragClient, new RagProperties(), gateway, ctx, registry,
                new SchemaValidator());
    }

    @Test
    void retrievesWhenIntentMatches() throws Exception {
        when(ragClient.search(anyString(), anyInt()))
                .thenReturn(List.of(new Hit("kafka-2", "kafka rebalance reassigns partitions", 0.9)));
        when(gateway.chat(any()))
                .thenReturn(new ChatResponse("{\"summary\":\"再均衡会重分配分区\"}", new TokenUsage(9, 5), "mock-small"));

        String out = processor().process("{\"question\":\"kafka 如何重分配分区?\"}");
        JsonNode node = om.readTree(out);
        assertTrue(node.get("usedRag").asBoolean());
        assertEquals("kafka-2", node.get("retrieved").get(0).asText());
        assertEquals("再均衡会重分配分区", node.get("answer").asText());
        verify(ragClient).search(anyString(), anyInt());
    }

    @Test
    void skipsRetrievalWhenIntentDoesNotMatch() throws Exception {
        when(gateway.chat(any()))
                .thenReturn(new ChatResponse("{\"summary\":\"ok\"}", new TokenUsage(2, 2), "mock-small"));

        String out = processor().process("{\"question\":\"hello world\"}");   // 无疑问/研究信号
        JsonNode node = om.readTree(out);
        assertFalse(node.get("usedRag").asBoolean());
        assertEquals(0, node.get("retrieved").size());
        verify(ragClient, never()).search(anyString(), anyInt());
    }

    @Test
    void typeIsResearchBatch() {
        assertEquals("RESEARCH_BATCH", processor().type());
    }
}
