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
 * 消费幂等:幂等键 = MD5(subtaskUuid + ":" + input),处理成功后把输出存进去(TTL 24h)。
 * 重复投递时不再干活,而是把存下的输出再回传一次(结果可能在路上丢了,见 SubtaskListener);
 * 失败不置键,合法重试可重跑。
 */
@Component
@RequiredArgsConstructor
public class IdempotencyGuard {

    private static final Duration TTL = Duration.ofHours(24);
    private static final String PREFIX = "agentflow:done:";   // 旧前缀 agentflow:idem: 下存的是 "1",不是输出

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

    /** 处理过 → 当时的输出;没处理过 → null。 */
    public String processedOutput(String key) {
        RBucket<String> bucket = redisson.getBucket(PREFIX + key);
        return bucket.get();
    }

    public void markProcessed(String key, String output) {
        RBucket<String> bucket = redisson.getBucket(PREFIX + key);
        bucket.set(output, TTL);
    }
}
