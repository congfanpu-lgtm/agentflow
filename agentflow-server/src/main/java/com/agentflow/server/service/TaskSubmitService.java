package com.agentflow.server.service;

import com.agentflow.common.state.SubtaskStatus;
import com.agentflow.common.state.TaskStatus;
import com.agentflow.server.coordinator.SubtaskDef;
import com.agentflow.server.coordinator.TaskDecomposer;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Collectors;

/** 协调器入口:按 type 找拆解器 → 事务内落库 task + N 个 subtask(均 PENDING)。 */
@Service
public class TaskSubmitService {

    private final Map<String, TaskDecomposer> decomposers;
    private final TaskMapper taskMapper;
    private final SubtaskMapper subtaskMapper;

    public TaskSubmitService(List<TaskDecomposer> decomposerList,
                             TaskMapper taskMapper, SubtaskMapper subtaskMapper) {
        this.decomposers = decomposerList.stream()
                .collect(Collectors.toMap(TaskDecomposer::type, Function.identity()));
        this.taskMapper = taskMapper;
        this.subtaskMapper = subtaskMapper;
    }

    @Transactional
    public TaskEntity submit(String type, JsonNode payload) {
        TaskDecomposer decomposer = decomposers.get(type);
        if (decomposer == null) {
            throw new IllegalArgumentException("不支持的任务类型: " + type);
        }
        List<SubtaskDef> defs = decomposer.decompose(payload);

        TaskEntity task = new TaskEntity();
        task.setTaskUuid(UUID.randomUUID().toString());
        task.setType(type);
        task.setStatus(TaskStatus.PENDING.name());
        task.setPayload(payload == null ? null : payload.toString());
        task.setSubtaskTotal(defs.size());
        task.setSubtaskDone(0);
        task.setSubtaskFailed(0);
        taskMapper.insert(task);

        for (SubtaskDef def : defs) {
            SubtaskEntity sub = new SubtaskEntity();
            sub.setSubtaskUuid(UUID.randomUUID().toString());
            sub.setTaskId(task.getId());
            sub.setSeq(def.seq());
            sub.setStatus(SubtaskStatus.PENDING.name());
            sub.setInput(def.inputJson());
            subtaskMapper.insert(sub);
        }
        return task;
    }
}
