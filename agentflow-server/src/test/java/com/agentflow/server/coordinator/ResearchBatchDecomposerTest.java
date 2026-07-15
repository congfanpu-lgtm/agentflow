package com.agentflow.server.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ResearchBatchDecomposerTest {

    private final ObjectMapper om = new ObjectMapper();
    private final ResearchBatchDecomposer decomposer = new ResearchBatchDecomposer(om);

    @Test
    void fansOutQuestionsWithModel() throws Exception {
        List<SubtaskDef> defs = decomposer.decompose(
                om.readTree("{\"questions\":[\"q1\",\"q2\"],\"model\":\"mock-small\"}"));
        assertEquals(2, defs.size());
        var in0 = om.readTree(defs.get(0).inputJson());
        assertEquals("q1", in0.get("question").asText());
        assertEquals("mock-small", in0.get("model").asText());
    }

    @Test
    void rejectsEmptyQuestions() throws Exception {
        assertThrows(IllegalArgumentException.class,
                () -> decomposer.decompose(om.readTree("{\"questions\":[]}")));
    }

    @Test
    void typeIsResearchBatch() {
        assertEquals("RESEARCH_BATCH", decomposer.type());
    }
}
