package com.agentflow.worker.processor;

import com.agentflow.common.llm.ChatMessage;
import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import com.agentflow.common.llm.TokenUsage;
import com.agentflow.worker.llm.LlmGateway;
import com.agentflow.worker.llm.LlmModelProperties;
import com.agentflow.worker.skill.SchemaValidator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

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

    @Test
    void tellsModelToSplitNotAnswer() throws Exception {
        // 真实模型(Claude)拿到裸问题会直接作答而不是拆题:用户消息里必须明确"不要回答,只输出 JSON"
        run("{\"subQuestions\":[\"a\"]}");
        ArgumentCaptor<ChatRequest> req = ArgumentCaptor.forClass(ChatRequest.class);
        verify(gateway).chat(req.capture());
        List<ChatMessage> msgs = req.getValue().getMessages();
        assertEquals("kafka vs rabbitmq", msgs.get(1).getContent());
        assertTrue(msgs.get(0).getContent().contains("Today is " + java.time.LocalDate.now(java.time.ZoneOffset.UTC).toString()), "规划提示词应注入当前日期");   // 首条 user 仍是原问题(mock 依赖)
        String last = msgs.get(msgs.size() - 1).getContent();
        assertTrue(last.contains("不要直接回答") && last.contains("subQuestions"), last);
    }
}
