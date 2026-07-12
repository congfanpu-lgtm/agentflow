package com.agentflow.server.mapper;

import com.agentflow.server.entity.TaskEntity;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Transactional  // 测试后回滚,不留脏数据
class TaskMapperTest {

    @Autowired
    private TaskMapper taskMapper;

    private TaskEntity newTask() {
        TaskEntity t = new TaskEntity();
        t.setTaskUuid(UUID.randomUUID().toString());
        t.setType("ECHO_BATCH");
        t.setStatus("PENDING");
        t.setSubtaskTotal(3);
        t.setSubtaskDone(0);
        taskMapper.insert(t);
        return t;
    }

    @Test
    void casSucceedsWhenStatusMatches() {
        TaskEntity t = newTask();
        assertEquals(1, taskMapper.casStatus(t.getId(), "PENDING", "RUNNING"));
        assertEquals("RUNNING", taskMapper.selectById(t.getId()).getStatus());
    }

    @Test
    void casFailsWhenStatusStale() {
        TaskEntity t = newTask();
        taskMapper.casStatus(t.getId(), "PENDING", "RUNNING");
        // 用过期的 from 再试,必须失败——这就是防并发非法迁移
        assertEquals(0, taskMapper.casStatus(t.getId(), "PENDING", "RUNNING"));
    }

    @Test
    void incrementCountersAreAtomicAdds() {
        TaskEntity t = newTask();
        taskMapper.incrementDone(t.getId());
        taskMapper.incrementDone(t.getId());
        taskMapper.incrementFailed(t.getId());
        TaskEntity loaded = taskMapper.selectById(t.getId());
        assertEquals(2, loaded.getSubtaskDone());
        assertEquals(1, loaded.getSubtaskFailed());
    }
}
