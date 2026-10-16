package com.agentflow.worker.agent;

import com.agentflow.common.llm.*;
import com.agentflow.common.trace.TraceStage;
import com.agentflow.worker.agent.tool.AgentTool;
import com.agentflow.worker.llm.LlmGateway;
import com.agentflow.worker.llm.LlmModelProperties;
import com.agentflow.worker.trace.TraceEmitter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class AgentLoopTest {

    /** 可计数的假工具:返回固定文本或抛出指定异常。 */
    static class FakeTool implements AgentTool {
        int calls;
        String output = "标题: Kafka\nURL: https://en.wikipedia.org/wiki/Apache_Kafka\n摘要: log";
        RuntimeException fail;

        public String name() { return "search"; }
        public String description() { return "d"; }
        public String parametersSchema() { return "{\"type\":\"object\"}"; }
        public String run(JsonNode args) {
            calls++;
            if (fail != null) throw fail;
            return output;
        }
    }

    private final LlmGateway gateway = mock(LlmGateway.class);
    private final AgentCheckpoint checkpoint = mock(AgentCheckpoint.class);
    private final TraceEmitter trace = mock(TraceEmitter.class);
    private final FakeTool tool = new FakeTool();
    private final AgentRun run = new AgentRun(1L, 2L, "uuid-1", "what is kafka", "m");

    private AgentLoop loop() {
        LlmModelProperties props = new LlmModelProperties();
        LlmModelProperties.Model m = new LlmModelProperties.Model();
        m.setName("m");
        m.setPricePer1kTokens(1.0);
        props.setModels(List.of(m));
        when(checkpoint.load(anyString())).thenReturn(List.of());
        return new AgentLoop(gateway, List.of(tool), checkpoint, trace, props);
    }

    private static ChatResponse call(String id, String args) {
        ChatResponse r = new ChatResponse("思路 " + id, new TokenUsage(100, 10), "m");
        r.setToolCalls(List.of(new ToolCall(id, "search", args)));
        return r;
    }

    private static ChatResponse answer(String content) {
        return new ChatResponse(content, new TokenUsage(100, 10), "m");
    }

    private List<ChatRequest> requests(int n) {
        ArgumentCaptor<ChatRequest> c = ArgumentCaptor.forClass(ChatRequest.class);
        verify(gateway, times(n)).chat(c.capture());
        return c.getAllValues();
    }

    private List<String> detailsOf(TraceStage stage) {
        ArgumentCaptor<String> d = ArgumentCaptor.forClass(String.class);
        verify(trace, atLeast(0)).emit(anyString(), anyLong(), anyLong(), eq(stage), any(), d.capture());
        return d.getAllValues();
    }

    @Test
    void searchesThenAnswersWithVerifiedSource() {
        AgentLoop loop = loop();
        when(gateway.chat(any())).thenReturn(
                call("c1", "{\"q\":\"kafka\"}"),
                answer("{\"answer\":\"Kafka 是分布式日志\",\"sources\":[{\"title\":\"Kafka\","
                        + "\"url\":\"https://en.wikipedia.org/wiki/Apache_Kafka\"}]}"));

        ObjectNode out = loop.run(run);

        assertEquals("Kafka 是分布式日志", out.get("answer").asText());
        assertEquals("https://en.wikipedia.org/wiki/Apache_Kafka", out.at("/sources/0/url").asText());
        assertEquals(2, out.at("/usage/calls").asInt());
        assertEquals(2, out.get("steps").asInt());
        assertEquals(1, tool.calls);
        verify(checkpoint, times(1)).append(eq("uuid-1"), argThat(s -> s.step() == 1));
        List<ChatRequest> reqs = requests(2);
        assertEquals("search", reqs.get(0).getTools().get(0).name());
        assertEquals("tool", reqs.get(1).getMessages().get(3).getRole());
        assertEquals("c1", reqs.get(1).getMessages().get(3).getToolCallId());
        assertTrue(detailsOf(TraceStage.AGENT_STEP).get(0).contains("\"tool\":\"search\""));
    }

    @Test
    void fabricatedCitationIsDroppedAndTraced() {
        AgentLoop loop = loop();
        when(gateway.chat(any())).thenReturn(call("c1", "{}"),
                answer("{\"answer\":\"a\",\"sources\":[{\"title\":\"x\",\"url\":\"https://made.up/page\"}]}"));

        ObjectNode out = loop.run(run);

        assertEquals(0, out.get("sources").size());
        assertTrue(detailsOf(TraceStage.CITATION_DROPPED).get(0).contains("https://made.up/page"));
    }

    @Test
    void toolErrorIsReturnedToModelInsteadOfThrowing() {
        AgentLoop loop = loop();
        tool.fail = new IllegalStateException("boom");
        when(gateway.chat(any())).thenReturn(call("c1", "{}"), answer("{\"answer\":\"a\",\"sources\":[]}"));

        loop.run(run);

        String toolMsg = requests(2).get(1).getMessages().get(3).getContent();
        assertTrue(toolMsg.startsWith("ERROR: "));
        assertTrue(toolMsg.contains("boom"));
    }

    @Test
    void malformedArgumentsReturnedAsError() {
        AgentLoop loop = loop();
        when(gateway.chat(any())).thenReturn(call("c1", "{not json"), answer("{\"answer\":\"a\",\"sources\":[]}"));

        loop.run(run);

        assertEquals(0, tool.calls);
        assertTrue(requests(2).get(1).getMessages().get(3).getContent().startsWith("ERROR: "));
    }

    @Test
    void unknownToolReturnedAsError() {
        AgentLoop loop = loop();
        ChatResponse bad = new ChatResponse("t", new TokenUsage(1, 1), "m");
        bad.setToolCalls(List.of(new ToolCall("c1", "rm_rf", "{}")));
        when(gateway.chat(any())).thenReturn(bad, answer("{\"answer\":\"a\",\"sources\":[]}"));

        loop.run(run);

        assertTrue(requests(2).get(1).getMessages().get(3).getContent().contains("未知工具 rm_rf"));
    }

    @Test
    void repeatedIdenticalCallIsServedFromCache() {
        AgentLoop loop = loop();
        when(gateway.chat(any())).thenReturn(call("c1", "{\"q\":\"k\"}"), call("c2", "{\"q\":\"k\"}"),
                answer("{\"answer\":\"a\",\"sources\":[]}"));

        loop.run(run);

        assertEquals(1, tool.calls);
    }

    @Test
    void stepLimitForcesAnswerWithoutTools() {
        AgentLoop loop = loop();
        List<ChatResponse> script = new ArrayList<>();
        for (int i = 1; i <= AgentLoop.MAX_STEPS; i++) {
            script.add(call("c" + i, "{\"q\":\"" + i + "\"}"));
        }
        script.add(answer("{\"answer\":\"forced\",\"sources\":[]}"));
        when(gateway.chat(any())).thenReturn(script.get(0), script.subList(1, script.size()).toArray(new ChatResponse[0]));

        ObjectNode out = loop.run(run);

        assertEquals("forced", out.get("answer").asText());
        List<ChatRequest> reqs = requests(AgentLoop.MAX_STEPS + 1);
        assertNull(reqs.get(AgentLoop.MAX_STEPS).getTools());
        assertEquals(Prompts.FORCE_ANSWER, reqs.get(AgentLoop.MAX_STEPS).getMessages()
                .get(reqs.get(AgentLoop.MAX_STEPS).getMessages().size() - 1).getContent());
    }

    @Test
    void resumesFromCheckpointWithoutRerunningTools() {
        AgentLoop loop = loop();
        List<AgentStep> saved = new ArrayList<>();
        for (int n = 1; n <= 2; n++) {
            ChatMessage a = ChatMessage.assistant("t" + n, List.of(new ToolCall("c" + n, "search", "{\"q\":\"" + n + "\"}")));
            saved.add(new AgentStep(n, a, List.of(ChatMessage.tool("c" + n, "URL: https://ex.com/" + n)), new Usage()));
        }
        when(checkpoint.load("uuid-1")).thenReturn(saved);
        when(gateway.chat(any())).thenReturn(
                answer("{\"answer\":\"done\",\"sources\":[{\"title\":\"e\",\"url\":\"https://ex.com/2\"}]}"));

        ObjectNode out = loop.run(run);

        assertEquals(0, tool.calls);
        assertEquals(6, requests(1).get(0).getMessages().size());   // system + user + 2×(assistant + tool)
        assertEquals("https://ex.com/2", out.at("/sources/0/url").asText());
        assertEquals(3, out.get("steps").asInt());
        assertTrue(detailsOf(TraceStage.RESUMED).get(0).contains("\"fromStep\":2"));
    }

    @Test
    void finalAnswerInProseFallsBack() {
        AgentLoop loop = loop();
        when(gateway.chat(any())).thenReturn(answer("Kafka is a distributed log."));

        ObjectNode out = loop.run(run);

        assertEquals("Kafka is a distributed log.", out.get("answer").asText());
        assertEquals(0, out.get("sources").size());
    }

    @Test
    void finalAnswerInsideJsonFenceIsParsed() {
        AgentLoop loop = loop();
        when(gateway.chat(any())).thenReturn(answer("```json\n{\"answer\":\"ok\",\"sources\":[]}\n```"));

        assertEquals("ok", loop.run(run).get("answer").asText());
    }
}
