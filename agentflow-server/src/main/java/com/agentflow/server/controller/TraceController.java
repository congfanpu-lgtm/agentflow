package com.agentflow.server.controller;

import com.agentflow.server.controller.dto.TraceView;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.TaskMapper;
import com.agentflow.server.trace.TraceEventRepository;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/tasks")
@RequiredArgsConstructor
public class TraceController {

    private final TraceEventRepository repository;
    private final TaskMapper taskMapper;

    @GetMapping("/{taskUuid}/trace")
    public List<TraceView> trace(@PathVariable("taskUuid") String taskUuid) {
        TaskEntity task = taskMapper.selectOne(
                new QueryWrapper<TaskEntity>().eq("task_uuid", taskUuid));
        if (task == null) {
            return List.of();
        }
        return repository.findByTraceIdOrderByTimestampAsc(String.valueOf(task.getId())).stream()
                .map(d -> new TraceView(d.getStage(), d.getStatus(),
                        d.getSubtaskId(), d.getDetail(), d.getTimestamp()))
                .toList();
    }
}
