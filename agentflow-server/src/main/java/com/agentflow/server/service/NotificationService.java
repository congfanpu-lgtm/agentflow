package com.agentflow.server.service;

import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.policy.SideEffectPolicy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/** 代表性副作用:任务完成通知(模拟发报告/建 case)。必须经 Policy 闸 → 业务幂等。 */
@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationService {

    private final SideEffectPolicy policy;

    public void notifyTaskFinished(TaskEntity task) {
        String businessKey = "notify:" + task.getTaskUuid();
        policy.execute(businessKey, () ->
                log.info("📣 任务完成通知已发送 taskUuid={} status={}",
                        task.getTaskUuid(), task.getStatus()));
    }
}
