package com.agentflow.server.product;

import lombok.RequiredArgsConstructor;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import java.time.Duration;

/** 固定窗口计数限流(Redis):窗口内第 max+1 次起返回 false。用于注册、登录、测试推送这类低频入口。 */
@Component
@RequiredArgsConstructor
public class Throttle {

    private static final String PREFIX = "agentflow:throttle:";

    private final RedissonClient redisson;

    public boolean allow(String key, int max, Duration window) {
        RAtomicLong counter = redisson.getAtomicLong(PREFIX + key);
        long n = counter.incrementAndGet();
        // 第一次计数时设过期;remainTimeToLive<0 兜住"加了一但没来得及设过期"的情况,否则这个键会永远拦人
        if (n == 1 || counter.remainTimeToLive() < 0) {
            counter.expire(window);
        }
        return n <= max;
    }
}
