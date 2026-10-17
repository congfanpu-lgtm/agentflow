package com.agentflow.server.controller;

import com.agentflow.server.demo.DemoGuard;
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
    private final DemoGuard demoGuard;

    @PostMapping("/replay/{subtaskId}")
    public ResponseEntity<Map<String, String>> replay(@PathVariable("subtaskId") Long subtaskId) {
        demoGuard.forbidInDemo();   // 重放会按原类型、原模型再跑,绕过演示配额和预算
        recovery.replay(subtaskId);
        return ResponseEntity.accepted().body(Map.of("replayed", String.valueOf(subtaskId)));
    }

    @ExceptionHandler(DemoGuard.Rejected.class)
    public ResponseEntity<Map<String, String>> demoRejected(DemoGuard.Rejected e) {
        return ResponseEntity.status(e.status()).body(Map.of("error", e.getMessage()));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> bad(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
    }
}
