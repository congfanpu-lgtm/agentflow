package com.agentflow.server.service;

import com.agentflow.common.mq.ResultMessage;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Transactional
class ResultHandleServiceTest {

    @Autowired
    private ResultHandleService service;
    @Autowired
    private TaskMapper taskMapper;
    @Autowired
    private SubtaskMapper subtaskMapper;
    @MockBean
    private KafkaTemplate<String, Object> kafkaTemplate;

    /** 造一个 RUNNING 任务 + n 个 DISPATCHED 子任务,返回 [taskId, subId1, subId2...] */
    private List<Long> fixture(int n) {
        TaskEntity t = new TaskEntity();
        t.setTaskUuid(UUID.randomUUID().toString());
        t.setType("ECHO_BATCH");
        t.setStatus("RUNNING");
        t.setSubtaskTotal(n);
        t.setSubtaskDone(0);
        t.setSubtaskFailed(0);
        taskMapper.insert(t);
        List<Long> ids = new java.util.ArrayList<>(List.of(t.getId()));
        for (int i = 0; i < n; i++) {
            SubtaskEntity s = new SubtaskEntity();
            s.setSubtaskUuid(UUID.randomUUID().toString());
            s.setTaskId(t.getId());
            s.setSeq(i);
            s.setStatus("DISPATCHED");
            s.setInput("{\"text\":\"x\"}");
            subtaskMapper.insert(s);
            ids.add(s.getId());
        }
        return ids;
    }

    @Test
    void partialResultUpdatesProgressOnly() {
        List<Long> ids = fixture(2);
        service.handle(new ResultMessage(ids.get(0), ids.get(1), true,
                "{\"echo\":\"X\",\"length\":1}", null));

        TaskEntity t = taskMapper.selectById(ids.get(0));
        assertEquals(1, t.getSubtaskDone());
        assertEquals("RUNNING", t.getStatus());
        assertEquals("COMPLETED", subtaskMapper.selectById(ids.get(1)).getStatus());
    }

    @Test
    void lastResultCompletesTaskAndAggregates() {
        List<Long> ids = fixture(2);
        service.handle(new ResultMessage(ids.get(0), ids.get(1), true,
                "{\"echo\":\"A\",\"length\":1}", null));
        service.handle(new ResultMessage(ids.get(0), ids.get(2), true,
                "{\"echo\":\"B\",\"length\":1}", null));

        TaskEntity t = taskMapper.selectById(ids.get(0));
        assertEquals("COMPLETED", t.getStatus());
        assertEquals(2, t.getSubtaskDone());
        assertNotNull(t.getResult());
        assertTrue(t.getResult().contains("\"A\""));
        assertTrue(t.getResult().contains("\"B\""));
    }

    @Test
    void mixedResultsEndPartialFailedKeepingSuccessOutputs() {
        List<Long> ids = fixture(2);
        service.handle(new ResultMessage(ids.get(0), ids.get(1), true,
                "{\"echo\":\"A\",\"length\":1}", null));
        service.handle(new ResultMessage(ids.get(0), ids.get(2), false, null, "boom"));

        TaskEntity t = taskMapper.selectById(ids.get(0));
        assertEquals("PARTIAL_FAILED", t.getStatus());
        assertEquals(1, t.getSubtaskDone());
        assertEquals(1, t.getSubtaskFailed());
        // 部分失败语义:成功输出保留,失败原因可见
        assertNotNull(t.getResult());
        assertTrue(t.getResult().contains("\"A\""));
        assertTrue(t.getResult().contains("boom"));
        assertEquals("boom", subtaskMapper.selectById(ids.get(2)).getErrorMsg());
    }

    @Test
    void allFailedEndsFailed() {
        List<Long> ids = fixture(2);
        service.handle(new ResultMessage(ids.get(0), ids.get(1), false, null, "e1"));
        service.handle(new ResultMessage(ids.get(0), ids.get(2), false, null, "e2"));

        TaskEntity t = taskMapper.selectById(ids.get(0));
        assertEquals("FAILED", t.getStatus());
        assertEquals(2, t.getSubtaskFailed());
    }

    @Test
    void singleFailureAmongPendingKeepsTaskRunning() {
        List<Long> ids = fixture(2);
        service.handle(new ResultMessage(ids.get(0), ids.get(1), false, null, "boom"));

        // 还有子任务未落定,任务不能提前进终态
        assertEquals("RUNNING", taskMapper.selectById(ids.get(0)).getStatus());
        assertEquals("FAILED", subtaskMapper.selectById(ids.get(1)).getStatus());
    }
}
