package com.agentflow.server.product;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;

/**
 * 简报的两个闹钟:生成(默认美东 07:00 开盘前、18:00 收盘后各一次)和推送(每小时第 5 分钟看看轮到谁)。
 * 多台 server 都会响,靠 BriefService / DeliveryService 里的去重保证只做一次。
 */
@Slf4j
@Component
@ConditionalOnProperty(prefix = "agentflow.brief", name = "scheduler-enabled", havingValue = "true", matchIfMissing = true)
@RequiredArgsConstructor
public class BriefScheduler {

    private final BriefService briefService;
    private final DeliveryService deliveryService;

    @Scheduled(cron = "${agentflow.brief.cron:0 0 7,18 * * *}", zone = "${agentflow.brief.zone:America/New_York}")
    public void generate() {
        try {
            log.info("定时简报 发起={}", briefService.triggerDaily());
        } catch (RuntimeException e) {
            log.error("定时简报发起失败", e);
        }
    }

    @Scheduled(cron = "0 5 * * * *")
    public void deliver() {
        int sent = deliveryService.deliverDue(Instant.now());
        if (sent > 0) {
            log.info("简报推送 消息数={}", sent);
        }
    }
}
