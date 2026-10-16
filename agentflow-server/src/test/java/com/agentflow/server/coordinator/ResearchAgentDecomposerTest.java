package com.agentflow.server.coordinator;

import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ResearchAgentDecomposerTest {

    private final ObjectMapper om = new ObjectMapper();
    private final ResearchAgentDecomposer d = new ResearchAgentDecomposer(om);

    private static SubtaskEntity sub(int seq, String kind, String status, String input, String output) {
        SubtaskEntity s = new SubtaskEntity();
        s.setSeq(seq);
        s.setKind(kind);
        s.setStatus(status);
        s.setInput(input);
        s.setOutput(output);
        return s;
    }

    private static TaskEntity task() {
        TaskEntity t = new TaskEntity();
        t.setType("RESEARCH_AGENT");
        t.setPayload("{\"question\":\"kafka vs rabbitmq\"}");
        return t;
    }

    @Test
    void decomposeCreatesSinglePlanSubtask() throws Exception {
        List<SubtaskDef> defs = d.decompose(om.readTree("{\"question\":\"  kafka vs rabbitmq \"}"));
        assertEquals(1, defs.size());
        assertEquals("PLAN", defs.get(0).kind());
        assertEquals("kafka vs rabbitmq", om.readTree(defs.get(0).inputJson()).get("question").asText());
    }

    @Test
    void decomposeValidatesQuestion() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> d.decompose(om.readTree("{}")));
        assertThrows(IllegalArgumentException.class, () -> d.decompose(om.readTree("{\"question\":\" \"}")));
        assertThrows(IllegalArgumentException.class,
                () -> d.decompose(om.readTree("{\"question\":\"" + "x".repeat(501) + "\"}")));
        assertThrows(IllegalArgumentException.class, () -> d.decompose(null));
    }

    @Test
    void completedPlanFansOutAgents() throws Exception {
        List<SubtaskDef> next = d.nextStage(task(), List.of(
                sub(0, "PLAN", "COMPLETED", "{}", "{\"subQuestions\":[\"a\",\"b\",\"c\"]}")));
        assertEquals(3, next.size());
        assertEquals(List.of(1, 2, 3), next.stream().map(SubtaskDef::seq).toList());
        assertTrue(next.stream().allMatch(x -> "AGENT".equals(x.kind())));
        assertEquals("b", om.readTree(next.get(1).inputJson()).get("question").asText());
    }

    @Test
    void failedPlanStops() {
        assertTrue(d.nextStage(task(), List.of(sub(0, "PLAN", "FAILED", "{}", null))).isEmpty());
    }

    @Test
    void settledAgentsProduceOneSynthesizeWithSuccessfulFindingsOnly() throws Exception {
        List<SubtaskDef> next = d.nextStage(task(), List.of(
                sub(0, "PLAN", "COMPLETED", "{}", "{\"subQuestions\":[\"a\",\"b\"]}"),
                sub(1, "AGENT", "COMPLETED", "{\"question\":\"a\"}",
                        "{\"answer\":\"A1\",\"sources\":[{\"title\":\"K\",\"url\":\"https://k\"}]}"),
                sub(2, "AGENT", "FAILED", "{\"question\":\"b\"}", null)));
        assertEquals(1, next.size());
        assertEquals("SYNTHESIZE", next.get(0).kind());
        assertEquals(3, next.get(0).seq());
        var in = om.readTree(next.get(0).inputJson());
        assertEquals("kafka vs rabbitmq", in.get("question").asText());
        assertEquals(1, in.get("findings").size());
        assertEquals("a", in.at("/findings/0/subQuestion").asText());
        assertEquals("https://k", in.at("/findings/0/sources/0/url").asText());
    }

    @Test
    void allAgentsFailedOrSynthesizeExistsStops() {
        assertTrue(d.nextStage(task(), List.of(
                sub(0, "PLAN", "COMPLETED", "{}", "{\"subQuestions\":[\"a\"]}"),
                sub(1, "AGENT", "FAILED", "{\"question\":\"a\"}", null))).isEmpty());
        assertTrue(d.nextStage(task(), List.of(
                sub(0, "PLAN", "COMPLETED", "{}", "{\"subQuestions\":[\"a\"]}"),
                sub(1, "AGENT", "COMPLETED", "{\"question\":\"a\"}", "{\"answer\":\"x\",\"sources\":[]}"),
                sub(2, "SYNTHESIZE", "COMPLETED", "{}", "{\"report\":\"r\"}"))).isEmpty());
    }
}
