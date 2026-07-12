package com.agentflow.worker.listener;

import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.common.mq.Topics;
import com.agentflow.worker.idempotency.IdempotencyGuard;
import com.agentflow.worker.processor.EchoProcessor;
import com.agentflow.worker.retry.RetryRouter;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.springframework.kafka.core.KafkaTemplate;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SubtaskListenerIdempotencyTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, Object> template = mock(KafkaTemplate.class);
    private final EchoProcessor processor = spy(new EchoProcessor());
    private final IdempotencyGuard guard = mock(IdempotencyGuard.class);
    private final SubtaskListener listener =
            new SubtaskListener(processor, template, guard, mock(RetryRouter.class));

    @Test
    void skipsProcessingWhenAlreadyProcessed() throws Exception {
        when(guard.key(any(), any())).thenReturn("k");
        when(guard.alreadyProcessed("k")).thenReturn(true);
        listener.onMessage(new SubtaskMessage(1L, 10L, "u", "ECHO_BATCH", 0, "{\"text\":\"a\"}"), null);
        verify(processor, never()).process(any());
        verify(template, never()).send(eq(Topics.RESULT), anyString(), any());
        verify(guard, never()).markProcessed(any());
    }

    @Test
    void processesAndMarksWhenFirstTime() throws Exception {
        when(guard.key(any(), any())).thenReturn("k");
        when(guard.alreadyProcessed("k")).thenReturn(false);
        listener.onMessage(new SubtaskMessage(1L, 10L, "u", "ECHO_BATCH", 0, "{\"text\":\"hi\"}"), null);
        verify(processor).process("{\"text\":\"hi\"}");
        verify(template).send(eq(Topics.RESULT), eq("10"), any());
        verify(guard).markProcessed("k");
        // 崩溃安全:必须先发 RESULT 再置幂等键(见 SubtaskListener 注释)
        InOrder inOrder = inOrder(template, guard);
        inOrder.verify(template).send(eq(Topics.RESULT), eq("10"), any());
        inOrder.verify(guard).markProcessed("k");
    }
}
