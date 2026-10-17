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
        assertTrue(c.getValue().getMessages().get(0).getContent().contains("Today is " + java.time.LocalDate.now(java.time.ZoneOffset.UTC).toString()));
                assertTrue(c.getValue().getMessages().get(0).getContent().endsWith(com.agentflow.worker.agent.Prompts.ENGLISH));
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
