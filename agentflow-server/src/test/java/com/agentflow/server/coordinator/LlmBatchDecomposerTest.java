package com.agentflow.server.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class LlmBatchDecomposerTest {

    private final ObjectMapper om = new ObjectMapper();
    private final LlmBatchDecomposer decomposer = new LlmBatchDecomposer(om);

    @Test
    void fansOutItemsWithModelPassthrough() throws Exception {
        List<SubtaskDef> defs = decomposer.decompose(
                om.readTree("{\"items\":[\"a\",\"b\"],\"model\":\"mock-large\"}"));
        assertEquals(2, defs.size());
        var in0 = om.readTree(defs.get(0).inputJson());
        assertEquals("a", in0.get("text").asText());
        assertEquals("mock-large", in0.get("model").asText());
    }

    @Test
    void omitsModelWhenAbsent() throws Exception {
        List<SubtaskDef> defs = decomposer.decompose(om.readTree("{\"items\":[\"x\"]}"));
        assertFalse(om.readTree(defs.get(0).inputJson()).has("model"));
    }

    @Test
    void rejectsEmptyItems() throws Exception {
        assertThrows(IllegalArgumentException.class,
                () -> decomposer.decompose(om.readTree("{\"items\":[]}")));
    }

    @Test
    void typeIsLlmBatch() {
        assertEquals("LLM_BATCH", decomposer.type());
    }
}
