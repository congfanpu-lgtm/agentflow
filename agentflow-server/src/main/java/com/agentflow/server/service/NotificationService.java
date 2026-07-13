package com.agentflow.server.service;

import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.policy.SideEffectPolicy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * 代表性副作用:任务完成通知(模拟发报告/建 case)。必须经 Policy 闸 → 业务幂等。
 *
 * <p>W4 遗留修复:通知在**终态事务提交后**才发(afterCommit),避免"对未提交/可能回滚的状态
 * 提前通知"——接真外部副作用(邮件/建 case)时提前通知就是不可撤销的错报。无活动事务时
 * (防御兜底)直接执行。业务幂等键不变,只改"时机"不改"去重"。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationService {

    private final SideEffectPolicy policy;

    public void notifyTaskFinished(TaskEntity task) {
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    doNotify(task);
                }
            });
        } else {
            doNotify(task);   // 非事务上下文:直接执行(防御兜底)
        }
    }

    private void doNotify(TaskEntity task) {
        String businessKey = "notify:" + task.getTaskUuid();
        policy.execute(businessKey, () ->
                log.info("📣 任务完成通知已发送 taskUuid={} status={}",
                        task.getTaskUuid(), task.getStatus()));
    }
}
