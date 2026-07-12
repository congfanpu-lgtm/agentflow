package com.agentflow.server.scheduler;

import com.agentflow.server.service.TimeoutSweepService;
import lombok.RequiredArgsConstructor;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/** 定时触发超时兜底扫描;间隔通过配置项可调,默认 30s。 */
@Component
@ConditionalOnProperty(prefix = "agentflow.timeout", name = "scheduler-enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
public class TimeoutScheduler {
    private final TimeoutSweepService sweep;

    @Scheduled(fixedDelayString = "${agentflow.timeout.sweep-interval-ms:30000}")
    public void run() { sweep.sweep(); }
}
