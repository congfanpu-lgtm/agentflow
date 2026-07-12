package com.agentflow.server.service;

import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.LocalDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(properties = "agentflow.timeout.stuck-seconds=2")
class TimeoutSweepServiceTest {

    @Autowired private TimeoutSweepService sweep;
    @Autowired private TaskMapper taskMapper;
    @Autowired private SubtaskMapper subtaskMapper;
    @MockBean private KafkaTemplate<String, Object> kafkaTemplate;

    private Long taskId, subId;

    private void seedStuck(int redispatchCount) {
        TaskEntity t = new TaskEntity();
        t.setTaskUuid(UUID.randomUUID().toString());
        t.setType("ECHO_BATCH"); t.setStatus("RUNNING");
        t.setSubtaskTotal(1); t.setSubtaskDone(0); t.setSubtaskFailed(0);
        taskMapper.insert(t); taskId = t.getId();
        SubtaskEntity s = new SubtaskEntity();
        s.setSubtaskUuid(UUID.randomUUID().toString());
        s.setTaskId(taskId); s.setSeq(0); s.setStatus("DISPATCHED");
        s.setInput("{\"text\":\"x\"}"); s.setRedispatchCount(redispatchCount);
        subtaskMapper.insert(s); subId = s.getId();
        // 强制 updated_at 过期(绕过 ON UPDATE 自动刷新);测试用 stuck-seconds=2,30s 足够安全越过阈值
        subtaskMapper.setUpdatedAt(subId, LocalDateTime.now().minusSeconds(30));
    }

    @AfterEach
    void cleanup() {
        if (subId != null) subtaskMapper.deleteById(subId);
        if (taskId != null) taskMapper.deleteById(taskId);
    }

    @Test
    void redispatchesWhenUnderLimit() {
        seedStuck(0);
        sweep.sweep();
        SubtaskEntity s = subtaskMapper.selectById(subId);
        assertEquals("DISPATCHED", s.getStatus());       // 仍在跑
        assertEquals(1, s.getRedispatchCount());          // 重投计数 +1
    }

    @Test
    void failsWhenOverLimit() {
        seedStuck(3);                                     // 已达上限(MAX_REDISPATCH=3)
        sweep.sweep();
        assertEquals("FAILED", subtaskMapper.selectById(subId).getStatus());
        assertEquals("FAILED", taskMapper.selectById(taskId).getStatus()); // 单子任务全败→FAILED
    }
}
