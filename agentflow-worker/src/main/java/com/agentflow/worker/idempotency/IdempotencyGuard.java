package com.agentflow.worker.idempotency;

import lombok.RequiredArgsConstructor;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;

/**
 * 消费幂等:幂等键 = MD5(subtaskUuid + ":" + input),仅处理成功后置键(TTL 24h)。
 * 挡住"重复投递已成功消息"的重复干活;失败不置键,合法重试可重跑。
 */
@Component
@RequiredArgsConstructor
public class IdempotencyGuard {

    private static final Duration TTL = Duration.ofHours(24);
    private static final String PREFIX = "agentflow:idem:";

    private final RedissonClient redisson;

    public String key(String subtaskUuid, String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest((subtaskUuid + ":" + input).getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (Exception e) {
            throw new IllegalStateException("MD5 不可用", e);
        }
    }

    public boolean alreadyProcessed(String key) {
        return redisson.getBucket(PREFIX + key).isExists();
    }

    public void markProcessed(String key) {
        RBucket<String> bucket = redisson.getBucket(PREFIX + key);
        bucket.set("1", TTL);
    }
}
