package com.agentflow.worker.listener;

import com.agentflow.common.mq.ResultMessage;
import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.common.mq.Topics;
import com.agentflow.worker.idempotency.IdempotencyGuard;
import com.agentflow.worker.processor.EchoProcessor;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class SubtaskListenerTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, Object> template = mock(KafkaTemplate.class);
    private final IdempotencyGuard guard = mock(IdempotencyGuard.class);
    private final SubtaskListener listener = new SubtaskListener(new EchoProcessor(), template, guard);

    {
        when(guard.key(any(), any())).thenReturn("k");
        when(guard.alreadyProcessed("k")).thenReturn(false);
    }

    @Test
    void successProducesCompletedResult() {
        SubtaskMessage msg = new SubtaskMessage(1L, 10L, "uuid-1", "ECHO_BATCH", 0,
                "{\"text\":\"hi\"}");
        listener.onMessage(msg);

        ArgumentCaptor<ResultMessage> captor = ArgumentCaptor.forClass(ResultMessage.class);
        verify(template).send(eq(Topics.RESULT), eq("10"), captor.capture());
        ResultMessage result = captor.getValue();
        assertTrue(result.isSuccess());
        assertEquals(1L, result.getTaskId());
        assertEquals(10L, result.getSubtaskId());
        assertEquals("{\"echo\":\"HI\",\"length\":2}", result.getOutputJson());
    }

    @Test
    void processingFailureProducesFailedResult() {
        SubtaskMessage msg = new SubtaskMessage(1L, 11L, "uuid-2", "ECHO_BATCH", 1, "{}");
        listener.onMessage(msg);

        ArgumentCaptor<ResultMessage> captor = ArgumentCaptor.forClass(ResultMessage.class);
        verify(template).send(eq(Topics.RESULT), eq("11"), captor.capture());
        ResultMessage result = captor.getValue();
        assertFalse(result.isSuccess());
        assertNotNull(result.getErrorMsg());
    }
}
