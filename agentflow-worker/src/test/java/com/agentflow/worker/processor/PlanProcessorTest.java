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
