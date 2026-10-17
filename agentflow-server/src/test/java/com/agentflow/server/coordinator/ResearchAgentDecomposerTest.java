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

    /** 反思默认关闭(评测:深度 95%→95%,成本 +85%,延迟 ×1.9),payload.reflect=true 才开。 */
    private static TaskEntity reflecting() {
        TaskEntity t = task();
        t.setPayload("{\"question\":\"kafka vs rabbitmq\",\"reflect\":true}");
        return t;
    }

    private static final String A1 = "{\"answer\":\"A1\",\"sources\":[{\"title\":\"K\",\"url\":\"https://k\"}]}";

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


    // ---- 反思补漏:AGENT 全部落定 → REFLECT 找缺口 → 最多 2 个追问 AGENT → SYNTHESIZE

    @Test
    void settledAgentsGoToReflectWhenRequested() throws Exception {
        List<SubtaskDef> next = d.nextStage(reflecting(), List.of(
                sub(0, "PLAN", "COMPLETED", "{}", "{\"subQuestions\":[\"a\",\"b\"]}"),
                sub(1, "AGENT", "COMPLETED", "{\"question\":\"a\"}", A1),
                sub(2, "AGENT", "FAILED", "{\"question\":\"b\"}", null)));
        assertEquals(1, next.size());
        assertEquals("REFLECT", next.get(0).kind());
        assertEquals(3, next.get(0).seq());
        var in = om.readTree(next.get(0).inputJson());
        assertEquals("kafka vs rabbitmq", in.get("question").asText());
        assertEquals(1, in.get("findings").size());
        assertEquals("A1", in.at("/findings/0/answer").asText());
    }

    @Test
    void reflectGapsFanOutFollowUpAgents() throws Exception {
        List<SubtaskDef> next = d.nextStage(reflecting(), List.of(
                sub(0, "PLAN", "COMPLETED", "{}", "{\"subQuestions\":[\"a\"]}"),
                sub(1, "AGENT", "COMPLETED", "{\"question\":\"a\"}", A1),
                sub(2, "REFLECT", "COMPLETED", "{}", "{\"gaps\":[\"x\",\"y\",\"z\"]}")));
        assertEquals(List.of(3, 4), next.stream().map(SubtaskDef::seq).toList(), "最多 2 个追问");
        assertTrue(next.stream().allMatch(x -> "AGENT".equals(x.kind())));
        var in = om.readTree(next.get(0).inputJson());
        assertEquals("x", in.get("question").asText());
        assertTrue(in.get("followUp").asBoolean());
    }

    @Test
    void followUpQuestionsAreCappedLikeUserQuestions() throws Exception {
        String longGap = "x".repeat(2000);
        List<SubtaskDef> next = d.nextStage(reflecting(), List.of(
                sub(0, "PLAN", "COMPLETED", "{}", "{\"subQuestions\":[\"a\"]}"),
                sub(1, "AGENT", "COMPLETED", "{\"question\":\"a\"}", A1),
                sub(2, "REFLECT", "COMPLETED", "{}", "{\"gaps\":[\"" + longGap + "\"]}")));
        assertEquals(ResearchAgentDecomposer.MAX_QUESTION_CHARS,
                om.readTree(next.get(0).inputJson()).get("question").asText().length());
    }

    @Test
    void reflectWithoutGapsOrFailedGoesToSynthesize() throws Exception {
        for (SubtaskEntity reflect : List.of(sub(2, "REFLECT", "COMPLETED", "{}", "{\"gaps\":[]}"),
                sub(2, "REFLECT", "FAILED", "{}", null))) {
            List<SubtaskDef> next = d.nextStage(reflecting(), List.of(
                    sub(0, "PLAN", "COMPLETED", "{}", "{\"subQuestions\":[\"a\"]}"),
                    sub(1, "AGENT", "COMPLETED", "{\"question\":\"a\"}", A1), reflect));
            assertEquals(1, next.size(), "反思失败不应拖垮任务:直接合成");
            assertEquals("SYNTHESIZE", next.get(0).kind());
            assertEquals(3, next.get(0).seq());
            assertEquals(1, om.readTree(next.get(0).inputJson()).get("findings").size());
        }
    }

    @Test
    void followUpAgentsSettledGoToSynthesizeWithAllFindings() throws Exception {
        List<SubtaskDef> next = d.nextStage(reflecting(), List.of(
                sub(0, "PLAN", "COMPLETED", "{}", "{\"subQuestions\":[\"a\"]}"),
                sub(1, "AGENT", "COMPLETED", "{\"question\":\"a\"}", A1),
                sub(2, "REFLECT", "COMPLETED", "{}", "{\"gaps\":[\"x\",\"y\"]}"),
                sub(3, "AGENT", "COMPLETED", "{\"question\":\"x\",\"followUp\":true}",
                        "{\"answer\":\"X1\",\"sources\":[]}"),
                sub(4, "AGENT", "FAILED", "{\"question\":\"y\",\"followUp\":true}", null)));
        assertEquals(1, next.size());
        assertEquals("SYNTHESIZE", next.get(0).kind());
        assertEquals(5, next.get(0).seq());
        var f = om.readTree(next.get(0).inputJson()).get("findings");
        assertEquals(2, f.size());
        assertEquals("x", f.at("/1/subQuestion").asText());
    }
}
