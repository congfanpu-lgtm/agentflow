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

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;

@SpringBootTest
class DlqRecoveryServiceTest {

    @Autowired private DlqRecoveryService recovery;
    @Autowired private TaskMapper taskMapper;
    @Autowired private SubtaskMapper subtaskMapper;
    @MockBean private KafkaTemplate<String, Object> kafkaTemplate;

    private Long taskId, subId;

    @AfterEach
    void cleanup() {
        if (subId != null) subtaskMapper.deleteById(subId);
        if (taskId != null) taskMapper.deleteById(taskId);
    }

    @Test
    void replayResetsSubtaskAndTaskThenRedispatches() {
        TaskEntity t = new TaskEntity();
        t.setTaskUuid(UUID.randomUUID().toString());
        t.setType("ECHO_BATCH"); t.setStatus("FAILED");   // 已终态
        t.setSubtaskTotal(1); t.setSubtaskDone(0); t.setSubtaskFailed(1);
        t.setResult("[{\"seq\":0,\"status\":\"FAILED\"}]");
        taskMapper.insert(t); taskId = t.getId();
        SubtaskEntity s = new SubtaskEntity();
        s.setSubtaskUuid(UUID.randomUUID().toString());
        s.setTaskId(taskId); s.setSeq(0); s.setStatus("FAILED");
        s.setInput("{\"text\":\"x\"}"); s.setErrorMsg("boom"); s.setRedispatchCount(0);
        subtaskMapper.insert(s); subId = s.getId();

        recovery.replay(subId);

        SubtaskEntity s2 = subtaskMapper.selectById(subId);
        assertEquals("PENDING", s2.getStatus());          // 重置
        assertNull(s2.getErrorMsg());                      // 旧错误信息被清空
        TaskEntity t2 = taskMapper.selectById(taskId);
        assertEquals("RUNNING", t2.getStatus());          // 任务复活
        assertEquals(0, t2.getSubtaskFailed());           // 失败计数回退
        assertNull(t2.getResult());                        // 旧的 FAILED 结果被清空
        verify(kafkaTemplate).send(eq(com.agentflow.common.mq.Topics.SUBTASK),
                eq(String.valueOf(subId)), any());        // 重投主 topic
    }
}
