package com.agentflow.worker.llm;

import lombok.RequiredArgsConstructor;
import org.redisson.api.RScript;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.stereotype.Component;

import java.util.Collections;

/**
 * 分布式令牌桶(Redis + Lua)。桶状态存 Redis(跨 worker 实例共享)——多实例水平扩展时
 * 限流才是"全局的"而非各 JVM 各限一份。
 *
 * <p>为什么用 Lua:令牌桶的"读令牌 → 按时间补充 → 判够不够 → 扣减 → 回写"是 check-then-act,
 * 并发下必须原子。Redis 单线程执行整段脚本 = 天然原子,免分布式锁(与 W3 状态机用 MySQL CAS
 * 把 read-modify-write 压成一个原子操作同源)。
 */
@Component
@RequiredArgsConstructor
public class RateLimiter {

    private static final String PREFIX = "agentflow:ratelimit:";

    /** 标准令牌桶:KEYS[1]=bucket;ARGV=capacity, refillPerSec, nowMs。够则扣 1 返回 1,否则 0。 */
    private static final String LUA = """
        local b = redis.call('HMGET', KEYS[1], 'tokens', 'ts')
        local cap = tonumber(ARGV[1])
        local refill = tonumber(ARGV[2])
        local now = tonumber(ARGV[3])
        local tokens = tonumber(b[1])
        local ts = tonumber(b[2])
        if tokens == nil then tokens = cap; ts = now end
        local delta = math.max(0, now - ts) / 1000.0 * refill
        tokens = math.min(cap, tokens + delta)
        local ok = 0
        if tokens >= 1 then tokens = tokens - 1; ok = 1 end
        redis.call('HMSET', KEYS[1], 'tokens', tokens, 'ts', now)
        redis.call('PEXPIRE', KEYS[1], 60000)
        return ok
        """;

    private final RedissonClient redisson;

    /**
     * 尝试取一个令牌。now 用调用方墙钟(单 Redis 实例足够;跨节点时钟漂移列 backlog,可改 Redis TIME)。
     * ponytail: 墙钟传入,精度足够演示;若上生产多写节点需换服务端时钟。
     */
    public boolean tryAcquire(String model, int capacity, double refillPerSec) {
        RScript script = redisson.getScript(StringCodec.INSTANCE);
        Long ok = script.eval(RScript.Mode.READ_WRITE, LUA, RScript.ReturnType.INTEGER,
                Collections.singletonList(PREFIX + model),
                capacity, refillPerSec, System.currentTimeMillis());
        return ok != null && ok == 1L;
    }
}
