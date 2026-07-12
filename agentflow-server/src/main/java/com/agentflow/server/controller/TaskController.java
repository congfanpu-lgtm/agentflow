package com.agentflow.server.controller;

import com.agentflow.server.controller.dto.SubmitTaskRequest;
import com.agentflow.server.controller.dto.TaskView;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.service.TaskQueryService;
import com.agentflow.server.service.TaskSubmitService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/tasks")
@RequiredArgsConstructor
public class TaskController {

    private final TaskSubmitService submitService;
    private final TaskQueryService queryService;

    @PostMapping
    public ResponseEntity<Map<String, String>> submit(@RequestBody SubmitTaskRequest req) {
        TaskEntity task = submitService.submit(req.getType(), req.getPayload());
        return ResponseEntity.status(HttpStatus.ACCEPTED)
                .body(Map.of("taskUuid", task.getTaskUuid()));
    }

    @GetMapping("/{taskUuid}")
    public ResponseEntity<TaskView> get(@PathVariable("taskUuid") String taskUuid) {
        TaskView view = queryService.findByUuid(taskUuid);
        return view == null ? ResponseEntity.notFound().build() : ResponseEntity.ok(view);
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> badRequest(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
    }
}
