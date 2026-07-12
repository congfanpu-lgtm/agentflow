package com.agentflow.server.trace;

import com.agentflow.common.trace.TraceStage;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class TraceConsumerTest {

    @Autowired private TraceEmitter emitter;
    @Autowired private TraceEventRepository repository;

    private String traceId;

    @AfterEach
    void cleanup() {
        if (traceId != null) {
            repository.deleteAll(repository.findByTraceIdOrderByTimestampAsc(traceId));
        }
    }

    @Test
    void emittedEventLandsInMongoInOrder() {
        traceId = "trace-" + UUID.randomUUID();
        emitter.emit(traceId, 1L, null, TraceStage.SUBMITTED, "PENDING", "submit");
        emitter.emit(traceId, 1L, 10L, TraceStage.DISPATCHED, "DISPATCHED", "seq0");

        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var events = repository.findByTraceIdOrderByTimestampAsc(traceId);
            assertEquals(2, events.size());
            assertEquals("SUBMITTED", events.get(0).getStage());
            assertEquals("DISPATCHED", events.get(1).getStage());
        });
    }
}
