package com.agentflow.server.service;

import com.agentflow.common.state.TaskStatus;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.TaskMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Transactional
class TaskStateMachineTest {

    @Autowired
    private TaskStateMachine stateMachine;
    @Autowired
    private TaskMapper taskMapper;

    private Long insertTask(String status) {
        TaskEntity t = new TaskEntity();
        t.setTaskUuid(UUID.randomUUID().toString());
        t.setType("ECHO_BATCH");
        t.setStatus(status);
        t.setSubtaskTotal(1);
        t.setSubtaskDone(0);
        taskMapper.insert(t);
        return t.getId();
    }

    @Test
    void legalTransitionPersists() {
        Long id = insertTask("PENDING");
        assertTrue(stateMachine.transitionTask(id, TaskStatus.PENDING, TaskStatus.RUNNING));
        assertEquals("RUNNING", taskMapper.selectById(id).getStatus());
    }

    @Test
    void illegalTransitionThrows() {
        Long id = insertTask("PENDING");
        assertThrows(IllegalStateException.class,
                () -> stateMachine.transitionTask(id, TaskStatus.PENDING, TaskStatus.COMPLETED));
    }

    @Test
    void staleCasReturnsFalse() {
        Long id = insertTask("RUNNING");
        // 数据库里已是 RUNNING,再按 PENDING→RUNNING 迁移应竞争失败而非抛错
        assertFalse(stateMachine.transitionTask(id, TaskStatus.PENDING, TaskStatus.RUNNING));
    }
}
