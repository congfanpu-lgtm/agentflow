package com.agentflow.server.controller;

import com.agentflow.server.service.DlqRecoveryService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/dlq")
@RequiredArgsConstructor
public class DlqController {

    private final DlqRecoveryService recovery;

    @PostMapping("/replay/{subtaskId}")
    public ResponseEntity<Map<String, String>> replay(@PathVariable("subtaskId") Long subtaskId) {
        recovery.replay(subtaskId);
        return ResponseEntity.accepted().body(Map.of("replayed", String.valueOf(subtaskId)));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> bad(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
    }
}
