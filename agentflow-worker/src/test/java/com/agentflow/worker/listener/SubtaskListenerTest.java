package com.agentflow.worker.listener;

import com.agentflow.worker.retry.NonRetryableException;
import com.agentflow.worker.processor.SubtaskProcessor;
import com.agentflow.common.mq.ResultMessage;
import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.common.mq.Topics;
import com.agentflow.worker.idempotency.IdempotencyGuard;
import com.agentflow.worker.processor.EchoProcessor;
import com.agentflow.worker.retry.RetryRouter;
import com.agentflow.worker.trace.TraceEmitter;
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
    private final RetryRouter retryRouter = mock(RetryRouter.class);
    private final SubtaskListener listener =
            new SubtaskListener(java.util.List.of(new EchoProcessor()), template, guard, retryRouter,
                    mock(TraceEmitter.class));

    {
        when(guard.key(any(), any())).thenReturn("k");
    }

    @Test
    void successProducesCompletedResult() {
        SubtaskMessage msg = new SubtaskMessage(1L, 10L, "uuid-1", "ECHO_BATCH", 0,
                "{\"text\":\"hi\"}");
        listener.onMessage(msg, null);

        ArgumentCaptor<ResultMessage> captor = ArgumentCaptor.forClass(ResultMessage.class);
        verify(template).send(eq(Topics.RESULT), eq("10"), captor.capture());
        ResultMessage result = captor.getValue();
        assertTrue(result.isSuccess());
        assertEquals(1L, result.getTaskId());
        assertEquals(10L, result.getSubtaskId());
        assertEquals("{\"echo\":\"HI\",\"length\":2}", result.getOutputJson());
    }

    @Test
    void processingFailureRoutesToRetryRouter() {
        SubtaskMessage msg = new SubtaskMessage(1L, 11L, "uuid-2", "ECHO_BATCH", 1, "{}");
        listener.onMessage(msg, null);

        verify(retryRouter).route(eq(msg), eq(0), any());
        verify(template, never()).send(eq(Topics.RESULT), anyString(), any());
    }

    @Test
    void failingToStoreTheIdempotencyKeyAfterSendingDoesNotRetry() {
        // Redis 挂掉时:结果已发出,不能因为幂等键没写上就走重试(那等于保证重复执行一次)
        org.mockito.Mockito.doThrow(new IllegalStateException("redis down")).when(guard).markProcessed(any(), any());
        SubtaskMessage msg = new SubtaskMessage(1L, 12L, "uuid-3", "ECHO_BATCH", 0, "{\"text\":\"hi\"}");

        listener.onMessage(msg, null);

        verify(template).send(eq(Topics.RESULT), eq("12"), any());
        verify(retryRouter, never()).route(any(), org.mockito.ArgumentMatchers.anyInt(), any());
    }

    @Test
    void aFailureThatRetryingCannotFixIsReportedAtOnce() {
        SubtaskProcessor bad = new SubtaskProcessor() {
            @Override
            public String type() {
                return "BAD_INPUT";
            }

            @Override
            public String process(String inputJson) {
                throw new NonRetryableException("media 400: only YouTube links are accepted");
            }
        };
        SubtaskListener l = new SubtaskListener(java.util.List.of(bad), template, guard, retryRouter,
                mock(TraceEmitter.class));
        l.onMessage(new SubtaskMessage(1L, 13L, "uuid-4", "BAD_INPUT", 0, "{}"), null);
        ArgumentCaptor<ResultMessage> captor = ArgumentCaptor.forClass(ResultMessage.class);
        verify(template).send(eq(Topics.RESULT), eq("13"), captor.capture());
        assertFalse(captor.getValue().isSuccess());
        assertEquals("media 400: only YouTube links are accepted", captor.getValue().getErrorMsg());
        verify(retryRouter, never()).route(any(), org.mockito.ArgumentMatchers.anyInt(), any());
    }
}
