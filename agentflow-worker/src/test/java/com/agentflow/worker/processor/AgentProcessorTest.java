package com.agentflow.worker.processor;

import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.worker.agent.AgentLoop;
import com.agentflow.worker.agent.AgentRun;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

class AgentProcessorTest {

    @Test
    void passesSubtaskIdentityToLoop() throws Exception {
        AgentLoop loop = mock(AgentLoop.class);
        when(loop.run(any())).thenReturn(new ObjectMapper().createObjectNode().put("answer", "x"));

        String out = new AgentProcessor(loop).process(
                new SubtaskMessage(7L, 8L, "uuid-8", "AGENT", 1, "{\"question\":\"q\",\"model\":\"m\"}"));

        assertTrue(out.contains("\"answer\":\"x\""));
        verify(loop).run(new AgentRun(7L, 8L, "uuid-8", "q", "m"));
    }

    @Test
    void rawJsonEntryPointIsRejected() {
        assertThrows(UnsupportedOperationException.class,
                () -> new AgentProcessor(mock(AgentLoop.class)).process("{\"question\":\"q\"}"));
    }
}
