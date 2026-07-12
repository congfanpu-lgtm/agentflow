package com.agentflow.server.controller;

import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.TaskMapper;
import com.agentflow.server.trace.TraceDocument;
import com.agentflow.server.trace.TraceEventRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import java.util.UUID;

import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
class TraceControllerTest {

    @Autowired private MockMvc mockMvc;
    @Autowired private TraceEventRepository repository;
    @Autowired private TaskMapper taskMapper;

    private String traceId;   // = String.valueOf(taskId)
    private Long taskId;

    @AfterEach
    void cleanup() {
        if (traceId != null) repository.deleteAll(repository.findByTraceIdOrderByTimestampAsc(traceId));
        if (taskId != null) taskMapper.deleteById(taskId);
    }

    private TraceDocument doc(String tid, String stage, long ts) {
        TraceDocument d = new TraceDocument();
        d.setTraceId(tid); d.setStage(stage); d.setStatus("X"); d.setTimestamp(ts);
        return d;
    }

    @Test
    void returnsTraceInTimestampOrder() throws Exception {
        // 造一个真实 task(controller 按 uuid 反查 id)
        TaskEntity t = new TaskEntity();
        String uuid = UUID.randomUUID().toString();
        t.setTaskUuid(uuid); t.setType("ECHO_BATCH"); t.setStatus("COMPLETED");
        t.setSubtaskTotal(1); t.setSubtaskDone(1); t.setSubtaskFailed(0);
        taskMapper.insert(t);
        taskId = t.getId();
        traceId = String.valueOf(taskId);
        repository.save(doc(traceId, "SUBMITTED", 100));
        repository.save(doc(traceId, "TASK_FINALIZED", 200));

        mockMvc.perform(get("/api/v1/tasks/" + uuid + "/trace"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(2)))
                .andExpect(jsonPath("$[0].stage", is("SUBMITTED")))
                .andExpect(jsonPath("$[1].stage", is("TASK_FINALIZED")));
    }

    @Test
    void unknownTraceReturnsEmpty() throws Exception {
        mockMvc.perform(get("/api/v1/tasks/no-such/trace"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(0)));
    }
}
