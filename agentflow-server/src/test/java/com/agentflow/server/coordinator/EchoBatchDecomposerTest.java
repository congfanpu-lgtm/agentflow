package com.agentflow.server.coordinator;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class EchoBatchDecomposerTest {

    private final ObjectMapper om = new ObjectMapper();
    private final EchoBatchDecomposer decomposer = new EchoBatchDecomposer(om);

    @Test
    void splitsItemsIntoOrderedSubtasks() throws Exception {
        var payload = om.readTree("{\"items\":[\"hello\",\"world\"]}");
        List<SubtaskDef> defs = decomposer.decompose(payload);
        assertEquals(2, defs.size());
        assertEquals(0, defs.get(0).seq());
        assertEquals("{\"text\":\"hello\"}", defs.get(0).inputJson());
        assertEquals(1, defs.get(1).seq());
        assertEquals("{\"text\":\"world\"}", defs.get(1).inputJson());
    }

    @Test
    void rejectsEmptyOrMissingItems() throws Exception {
        var empty = om.readTree("{\"items\":[]}");
        assertThrows(IllegalArgumentException.class, () -> decomposer.decompose(empty));
        var missing = om.readTree("{}");
        assertThrows(IllegalArgumentException.class, () -> decomposer.decompose(missing));
    }
}
