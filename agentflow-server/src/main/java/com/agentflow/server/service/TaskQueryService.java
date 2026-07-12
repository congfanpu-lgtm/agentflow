package com.agentflow.server.service;

import com.agentflow.server.controller.dto.TaskView;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
@RequiredArgsConstructor
public class TaskQueryService {

    private final TaskMapper taskMapper;
    private final SubtaskMapper subtaskMapper;
    private final ObjectMapper objectMapper;

    /** 按 uuid 查任务视图;不存在返回 null(controller 转 404)。 */
    public TaskView findByUuid(String taskUuid) {
        TaskEntity task = taskMapper.selectOne(
                new QueryWrapper<TaskEntity>().eq("task_uuid", taskUuid));
        if (task == null) {
            return null;
        }
        List<SubtaskEntity> subs = subtaskMapper.selectList(
                new QueryWrapper<SubtaskEntity>()
                        .eq("task_id", task.getId()).orderByAsc("seq"));
        var subViews = subs.stream()
                .map(s -> new TaskView.SubtaskView(s.getSeq(), s.getStatus(), s.getErrorMsg()))
                .toList();
        com.fasterxml.jackson.databind.JsonNode result = null;
        try {
            result = task.getResult() == null ? null : objectMapper.readTree(task.getResult());
        } catch (Exception ignored) {
        }
        return new TaskView(task.getTaskUuid(), task.getType(), task.getStatus(),
                new TaskView.Progress(task.getSubtaskTotal(), task.getSubtaskDone(),
                        task.getSubtaskFailed()),
                result, subViews);
    }
}
