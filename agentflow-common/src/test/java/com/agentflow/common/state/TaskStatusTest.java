package com.agentflow.common.state;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TaskStatusTest {

    @Test
    void pendingCanOnlyGoToRunningOrFailed() {
        assertTrue(TaskStatus.PENDING.canTransitionTo(TaskStatus.RUNNING));
        assertTrue(TaskStatus.PENDING.canTransitionTo(TaskStatus.FAILED));
        assertFalse(TaskStatus.PENDING.canTransitionTo(TaskStatus.COMPLETED));
        assertFalse(TaskStatus.PENDING.canTransitionTo(TaskStatus.PENDING));
    }

    @Test
    void runningCanGoToAnyTerminal() {
        assertTrue(TaskStatus.RUNNING.canTransitionTo(TaskStatus.COMPLETED));
        assertTrue(TaskStatus.RUNNING.canTransitionTo(TaskStatus.PARTIAL_FAILED));
        assertTrue(TaskStatus.RUNNING.canTransitionTo(TaskStatus.FAILED));
        assertFalse(TaskStatus.RUNNING.canTransitionTo(TaskStatus.PENDING));
    }

    @Test
    void terminalStatesAreFinal() {
        for (TaskStatus target : TaskStatus.values()) {
            assertFalse(TaskStatus.COMPLETED.canTransitionTo(target));
            assertFalse(TaskStatus.PARTIAL_FAILED.canTransitionTo(target));
            assertFalse(TaskStatus.FAILED.canTransitionTo(target));
        }
    }
}
