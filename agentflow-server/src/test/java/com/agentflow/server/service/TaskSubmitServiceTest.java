package com.agentflow.server.service;

import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
@Transactional
class TaskSubmitServiceTest {

    @Autowired
    private TaskSubmitService service;
    @Autowired
    private TaskMapper taskMapper;
    @Autowired
    private SubtaskMapper subtaskMapper;
    @Autowired
    private ObjectMapper om;

    @Test
    void submitPersistsTaskAndSubtasks() throws Exception {
        var payload = om.readTree("{\"items\":[\"a\",\"b\",\"c\"]}");
        TaskEntity task = service.submit("ECHO_BATCH", payload);

        assertNotNull(task.getId());
        assertNotNull(task.getTaskUuid());
        assertEquals("PENDING", task.getStatus());
        assertEquals(3, task.getSubtaskTotal());

        List<SubtaskEntity> subs = subtaskMapper.selectList(
                new QueryWrapper<SubtaskEntity>().eq("task_id", task.getId()).orderByAsc("seq"));
        assertEquals(3, subs.size());
        assertEquals("PENDING", subs.get(0).getStatus());
        assertEquals("{\"text\": \"a\"}".replace(" ", ""),
                subs.get(0).getInput().replace(" ", ""));
    }

    @Test
    void unknownTypeThrows() throws Exception {
        var payload = om.readTree("{\"items\":[\"a\"]}");
        assertThrows(IllegalArgumentException.class, () -> service.submit("NOPE", payload));
    }
}
