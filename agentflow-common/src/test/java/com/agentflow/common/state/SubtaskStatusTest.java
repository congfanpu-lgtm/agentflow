package com.agentflow.common.state;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SubtaskStatusTest {

    @Test
    void legalPath() {
        assertTrue(SubtaskStatus.PENDING.canTransitionTo(SubtaskStatus.DISPATCHED));
        assertTrue(SubtaskStatus.DISPATCHED.canTransitionTo(SubtaskStatus.COMPLETED));
        assertTrue(SubtaskStatus.DISPATCHED.canTransitionTo(SubtaskStatus.FAILED));
    }

    @Test
    void illegalJumps() {
        assertFalse(SubtaskStatus.PENDING.canTransitionTo(SubtaskStatus.COMPLETED));
        assertFalse(SubtaskStatus.COMPLETED.canTransitionTo(SubtaskStatus.FAILED));
        assertFalse(SubtaskStatus.FAILED.canTransitionTo(SubtaskStatus.DISPATCHED));
    }
}
