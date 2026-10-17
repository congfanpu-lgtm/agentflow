package com.agentflow.worker.processor;

import com.agentflow.common.llm.ChatMessage;
import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import com.agentflow.common.llm.TokenUsage;
import com.agentflow.worker.agent.Prompts;
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

/** 反思:读各 agent 结论,找出回答原问题还缺什么,给出最多 2 个追问。 */
class ReflectProcessorTest {

    private static final ObjectMapper OM = new ObjectMapper();
    private static final String INPUT = """
            {"question":"kafka vs rabbitmq","findings":[
              {"subQuestion":"throughput?","answer":"Kafka is faster","sources":[{"title":"K","url":"https://k"}]},
              {"subQuestion":"routing?","answer":"RabbitMQ routes flexibly","sources":[]}]}
            """;

    private final LlmGateway gateway = mock(LlmGateway.class);

    private ReflectProcessor processor() {
        LlmModelProperties props = new LlmModelProperties();
        LlmModelProperties.Model m = new LlmModelProperties.Model();
        m.setName("m");
        props.setModels(List.of(m));
        return new ReflectProcessor(gateway, props);
    }

    private JsonNode run(String llmOutput) throws Exception {
        when(gateway.chat(any())).thenReturn(new ChatResponse(llmOutput, new TokenUsage(5, 5), "m"));
        return OM.readTree(processor().process(INPUT));
    }

    private static List<?> gaps(JsonNode out) {
        return OM.convertValue(out.get("gaps"), List.class);
    }

    @Test
    void dedupesTrimsAndCapsGapsAtTwo() throws Exception {
        JsonNode out = run("```json\n{\"gaps\":[\" ordering guarantees? \",\"ordering guarantees?\",\"\",\"ops cost?\",\"x\"],"
                + "\"reason\":\"no ordering info\"}\n```");
        assertEquals(List.of("ordering guarantees?", "ops cost?"), gaps(out));
        assertEquals("no ordering info", out.get("reason").asText());
        assertEquals(1, out.at("/usage/calls").asInt());
    }

    @Test
    void sufficientFindingsMeanNoGaps() throws Exception {
        assertEquals(List.of(), gaps(run("{\"gaps\":[],\"reason\":\"covered\"}")));
    }

    @Test
    void unparseableOutputMeansNoGapsInsteadOfRetrying() throws Exception {
        // 反思是锦上添花:格式不对就当"无缺口"直接合成,不为它烧重试
        assertEquals(List.of(), gaps(run("Looks complete to me.")));
    }

    @Test
    void promptCarriesQuestionFindingsAndLanguage() throws Exception {
        run("{\"gaps\":[]}");
        ArgumentCaptor<ChatRequest> c = ArgumentCaptor.forClass(ChatRequest.class);
        verify(gateway).chat(c.capture());
        List<ChatMessage> msgs = c.getValue().getMessages();
        assertTrue(msgs.get(0).getContent().startsWith(Prompts.REFLECTOR_TAG));
        assertTrue(msgs.get(0).getContent().endsWith(Prompts.ENGLISH));
        String user = msgs.get(1).getContent();
        assertTrue(user.contains("kafka vs rabbitmq") && user.contains("### routing?\nRabbitMQ routes flexibly"), user);
    }
}
