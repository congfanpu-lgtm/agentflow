package com.agentflow.common.state;

import java.util.Map;
import java.util.Set;

/**
 * 任务状态机:PENDING → RUNNING → COMPLETED / PARTIAL_FAILED / FAILED。
 * PARTIAL_FAILED = 部分子任务成功(spec「部分失败语义」的 W1-2 批量版)。
 * 迁移合法性集中在此;W3-4 增补 RETRY/TIMEOUT 时只改这张表。
 */
public enum TaskStatus {
    PENDING, RUNNING, COMPLETED, PARTIAL_FAILED, FAILED;

    private static final Map<TaskStatus, Set<TaskStatus>> LEGAL = Map.of(
            PENDING, Set.of(RUNNING, FAILED),
            RUNNING, Set.of(COMPLETED, PARTIAL_FAILED, FAILED),
            COMPLETED, Set.of(),
            PARTIAL_FAILED, Set.of(),
            FAILED, Set.of()
    );

    public boolean canTransitionTo(TaskStatus target) {
        return LEGAL.get(this).contains(target);
    }
}
