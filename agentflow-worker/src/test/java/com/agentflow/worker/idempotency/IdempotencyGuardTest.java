package com.agentflow.worker.idempotency;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class IdempotencyGuardTest {

    @Autowired private IdempotencyGuard guard;

    @Test
    void sameContentSameKey() {
        String k1 = guard.key("uuid-1", "{\"text\":\"a\"}");
        String k2 = guard.key("uuid-1", "{\"text\":\"a\"}");
        assertEquals(k1, k2);
        assertNotEquals(k1, guard.key("uuid-2", "{\"text\":\"a\"}"));
    }

    @Test
    void markThenAlreadyProcessed() {
        String key = guard.key("uuid-" + System.nanoTime(), "{\"text\":\"a\"}");
        assertFalse(guard.alreadyProcessed(key));
        guard.markProcessed(key);
        assertTrue(guard.alreadyProcessed(key));
    }
}
