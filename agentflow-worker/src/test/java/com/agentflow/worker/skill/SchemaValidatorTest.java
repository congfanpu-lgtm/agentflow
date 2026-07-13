package com.agentflow.worker.skill;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class SchemaValidatorTest {

    private final SchemaValidator v = new SchemaValidator();
    private final ObjectMapper om = new ObjectMapper();

    private com.fasterxml.jackson.databind.JsonNode json(String s) throws Exception {
        return om.readTree(s);
    }

    @Test
    void passesWhenConformant() throws Exception {
        assertTrue(v.validate(json("{\"summary\":\"hi\"}"), Map.of("summary", "string")).isEmpty());
    }

    @Test
    void failsOnMissingField() throws Exception {
        var reasons = v.validate(json("{\"other\":1}"), Map.of("summary", "string"));
        assertEquals(1, reasons.size());
        assertTrue(reasons.get(0).contains("summary"));
    }

    @Test
    void failsOnWrongType() throws Exception {
        var reasons = v.validate(json("{\"summary\":123}"), Map.of("summary", "string"));
        assertFalse(reasons.isEmpty());
    }

    @Test
    void failsWhenNotObject() throws Exception {
        assertFalse(v.validate(json("[1,2]"), Map.of("summary", "string")).isEmpty());
    }
}
