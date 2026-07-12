package com.agentflow.common.state;

import java.util.Map;
import java.util.Set;

/** 子任务状态机:PENDING → DISPATCHED → COMPLETED/FAILED。 */
public enum SubtaskStatus {
    PENDING, DISPATCHED, COMPLETED, FAILED;

    private static final Map<SubtaskStatus, Set<SubtaskStatus>> LEGAL = Map.of(
            PENDING, Set.of(DISPATCHED, FAILED),
            DISPATCHED, Set.of(COMPLETED, FAILED),
            COMPLETED, Set.of(),
            FAILED, Set.of()
    );

    public boolean canTransitionTo(SubtaskStatus target) {
        return LEGAL.get(this).contains(target);
    }
}
