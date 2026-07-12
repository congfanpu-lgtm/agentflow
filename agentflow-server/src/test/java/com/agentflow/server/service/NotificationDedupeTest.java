package com.agentflow.server.service;

import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.policy.SideEffectPolicy;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class NotificationDedupeTest {

    @Autowired private NotificationService notificationService;
    @Autowired private SideEffectPolicy policy;

    @Test
    void sameTaskNotifiesOnce() {
        TaskEntity t = new TaskEntity();
        t.setTaskUuid(UUID.randomUUID().toString());
        t.setStatus("COMPLETED");
        // 真实调用两次 notifyTaskFinished,证明底层业务 key 只会被占用一次
        notificationService.notifyTaskFinished(t);
        notificationService.notifyTaskFinished(t);
        // 此时业务 key 应已被第一次调用占用;第三次尝试对同一 key 执行必须被去重跳过
        String key = "notify:" + t.getTaskUuid();
        assertFalse(policy.execute(key, () -> fail("should be deduped")));
    }
}
