package com.agentflow.server.trace;

import com.agentflow.server.service.TaskSubmitService;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.trace.TraceEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class ServerTracePointsTest {

    @Autowired private TaskSubmitService submitService;
    @Autowired private TraceEventRepository repository;
    @Autowired private ObjectMapper om;

    private String traceId;

    @AfterEach
    void cleanup() {
        if (traceId != null) repository.deleteAll(repository.findByTraceIdOrderByTimestampAsc(traceId));
    }

    @Test
    void submitEmitsSubmittedDecomposedDispatched() throws Exception {
        // 真实提交:会走 persist + dispatch(worker 可能不在,但 DISPATCHED 由 server 分发时 emit)
        TaskEntity task = submitService.submit("ECHO_BATCH",
                om.readTree("{\"items\":[\"a\",\"b\"]}"));
        traceId = String.valueOf(task.getId());

        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            List<TraceDocument> evs = repository.findByTraceIdOrderByTimestampAsc(traceId);
            Set<String> stages = evs.stream().map(TraceDocument::getStage).collect(Collectors.toSet());
            assertTrue(stages.contains("SUBMITTED"), "缺 SUBMITTED");
            assertTrue(stages.contains("DECOMPOSED"), "缺 DECOMPOSED");
            assertTrue(stages.contains("DISPATCHED"), "缺 DISPATCHED");
        });
    }
}
