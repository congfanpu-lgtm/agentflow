package com.agentflow.server.service;

import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.policy.SideEffectPolicy;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class NotificationDedupeTest {

    @Autowired private NotificationService notificationService;
    @Autowired private SideEffectPolicy policy;
    @Autowired private PlatformTransactionManager txManager;

    @Test
    void sameTaskNotifiesOnce() {
        TaskEntity t = new TaskEntity();
        t.setTaskUuid(UUID.randomUUID().toString());
        t.setStatus("COMPLETED");
        // 无事务上下文 → 直接执行两次,证明底层业务 key 只会被占用一次
        notificationService.notifyTaskFinished(t);
        notificationService.notifyTaskFinished(t);
        String key = "notify:" + t.getTaskUuid();
        assertFalse(policy.execute(key, () -> fail("should be deduped")));
    }

    @Test
    void notifyFiresOnlyAfterCommitNotOnRollback() {
        TaskEntity t = new TaskEntity();
        t.setTaskUuid(UUID.randomUUID().toString());
        t.setStatus("COMPLETED");
        String key = "notify:" + t.getTaskUuid();

        // 在事务内触发通知然后回滚 → afterCommit 不应执行 → 业务 key 未被占用
        new TransactionTemplate(txManager).executeWithoutResult(status -> {
            notificationService.notifyTaskFinished(t);
            status.setRollbackOnly();
        });
        // key 仍空:能成功占用 = 上面的通知确实没发(afterCommit 在回滚时被跳过)
        assertTrue(policy.execute(key, () -> {}), "回滚后不应已发通知");
    }
}
