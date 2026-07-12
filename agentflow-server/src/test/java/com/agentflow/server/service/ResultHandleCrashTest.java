package com.agentflow.server.service;

import com.agentflow.common.mq.ResultMessage;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doThrow;

// 注意:本类【不加】@Transactional —— 需要 handle 在自己的事务里提交/回滚,由测试直接观察 DB。
@SpringBootTest
class ResultHandleCrashTest {

    @Autowired private ResultHandleService service;
    @Autowired private TaskMapper taskMapper;
    @Autowired private SubtaskMapper subtaskMapper;
    @SpyBean private TaskMapper taskMapperSpy; // 与上面同一个 bean;用于打桩抛异常
    @MockBean private KafkaTemplate<String, Object> kafkaTemplate;

    private Long taskId;
    private Long subId;

    private void seed() {
        TaskEntity t = new TaskEntity();
        t.setTaskUuid(UUID.randomUUID().toString());
        t.setType("ECHO_BATCH");
        t.setStatus("RUNNING");
        t.setSubtaskTotal(1);
        t.setSubtaskDone(0);
        t.setSubtaskFailed(0);
        taskMapper.insert(t);
        taskId = t.getId();
        SubtaskEntity s = new SubtaskEntity();
        s.setSubtaskUuid(UUID.randomUUID().toString());
        s.setTaskId(taskId);
        s.setSeq(0);
        s.setStatus("DISPATCHED");
        s.setInput("{\"text\":\"x\"}");
        subtaskMapper.insert(s);
        subId = s.getId();
    }

    @AfterEach
    void cleanup() {
        if (subId != null) subtaskMapper.deleteById(subId);
        if (taskId != null) taskMapper.deleteById(taskId);
    }

    @Test
    void crashBetweenSettleAndIncrementRollsBackSubtask() {
        seed();
        // 在计数自增处注入崩溃(RuntimeException 默认触发回滚)
        doThrow(new RuntimeException("模拟崩溃")).when(taskMapperSpy).incrementDone(taskId);

        assertThrows(RuntimeException.class, () ->
                service.handle(new ResultMessage(taskId, subId, true,
                        "{\"echo\":\"X\",\"length\":1}", null)));

        // 关键断言:子任务状态回滚到 DISPATCHED(而非停留在 COMPLETED),计数未变
        assertEquals("DISPATCHED", subtaskMapper.selectById(subId).getStatus());
        TaskEntity t = taskMapper.selectById(taskId);
        assertEquals(0, t.getSubtaskDone());
        assertEquals("RUNNING", t.getStatus());
    }
}
