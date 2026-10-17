package com.agentflow.worker.llm;

import com.agentflow.common.llm.TokenUsage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.time.LocalDate;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.*;

/** TokenAccountant 集成测试(真 Redis,localhost:6379)。 */
class TokenAccountantTest {

    private static RedissonClient redisson;

    @BeforeAll
    static void connect() {
        Config c = new Config();
        c.useSingleServer().setAddress("redis://localhost:6379");
        redisson = Redisson.create(c);
    }

    @AfterAll
    static void close() {
        redisson.shutdown();
    }

    @Test
    void alsoAccumulatesTodaysSpendForTheDemoBudget() {
        // server 的演示保护按"当天总花费"拒绝新任务:worker 记账时顺手累加当天计数(UTC 日期,带过期)
        RAtomicLong day = redisson.getAtomicLong(TokenAccountant.DAILY_COST_PREFIX + LocalDate.now(ZoneOffset.UTC));
        long before = day.get();
        new TokenAccountant(redisson).record("test-model", new TokenUsage(600, 400), 0.002);   // 1000 token × $0.002/1k = $0.002 = 2000 微美元
        long delta = day.get() - before;
        day.addAndGet(-delta);   // 不污染真实的当天花费
        assertEquals(2000, delta);
        assertTrue(day.remainTimeToLive() > 0, "当天计数要带过期,不能永久留在 Redis");
    }
}
