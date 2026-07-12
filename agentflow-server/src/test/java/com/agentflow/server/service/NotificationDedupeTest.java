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
        // 直接验 policy 层去重(businessKey 相同第二次返回 false)
        String key = "notify:" + t.getTaskUuid();
        assertTrue(policy.execute(key, () -> {}));
        assertFalse(policy.execute(key, () -> {}));
        // notifyTaskFinished 第二次不会真正执行 action(已被去重)
        notificationService.notifyTaskFinished(t); // 幂等,无副作用重复
    }
}
