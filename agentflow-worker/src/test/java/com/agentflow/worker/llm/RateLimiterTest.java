package com.agentflow.worker.llm;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** 令牌桶集成测试(真 Redis)。每次用唯一 model 名避免复用旧桶。 */
class RateLimiterTest {

    private static RedissonClient redisson;
    private RateLimiter limiter;

    @BeforeAll
    static void up() {
        Config cfg = new Config();
        cfg.useSingleServer().setAddress("redis://localhost:6379");
        redisson = Redisson.create(cfg);
    }

    @AfterAll
    static void down() {
        redisson.shutdown();
    }

    @Test
    void exhaustsBucketThenRefuses() {
        limiter = new RateLimiter(redisson);
        String model = "rl-" + UUID.randomUUID();       // capacity=2, 不补充
        assertTrue(limiter.tryAcquire(model, 2, 0));
        assertTrue(limiter.tryAcquire(model, 2, 0));
        assertFalse(limiter.tryAcquire(model, 2, 0));    // 第三次:桶空
    }

    @Test
    void refillsOverTime() throws InterruptedException {
        limiter = new RateLimiter(redisson);
        String model = "rl-" + UUID.randomUUID();       // capacity=1, 5 令牌/秒(200ms 补 1 个)
        assertTrue(limiter.tryAcquire(model, 1, 5));
        assertFalse(limiter.tryAcquire(model, 1, 5));    // 立即再取:空(两次调用间隔远小于 200ms)
        Thread.sleep(300);                                // 300ms * 5/s = 1.5 令牌(封顶 1)
        assertTrue(limiter.tryAcquire(model, 1, 5));      // 已补充
    }
}
