package com.agentflow.server.policy;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class SideEffectPolicyTest {

    @Autowired private SideEffectPolicy policy;

    @Test
    void runsOnceThenDedupes() {
        String key = "test:" + UUID.randomUUID();
        AtomicInteger n = new AtomicInteger();
        assertTrue(policy.execute(key, n::incrementAndGet));   // 首次执行
        assertFalse(policy.execute(key, n::incrementAndGet));  // 去重跳过
        assertEquals(1, n.get());                              // 只执行一次
    }
}
