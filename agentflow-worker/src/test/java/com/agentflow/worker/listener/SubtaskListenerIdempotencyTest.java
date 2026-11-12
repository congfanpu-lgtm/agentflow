package com.agentflow.worker.listener;

import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.common.mq.Topics;
import com.agentflow.worker.idempotency.IdempotencyGuard;
import com.agentflow.worker.processor.EchoProcessor;
import com.agentflow.worker.retry.RetryRouter;
import com.agentflow.worker.trace.TraceEmitter;
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
            new SubtaskListener(java.util.List.of(processor), template, guard,
                    mock(RetryRouter.class), mock(TraceEmitter.class));

    @Test
    void redeliveryDoesNotReprocessButResendsTheStoredResult() throws Exception {
        // 结果丢过(发送失败 / server 处理出错被丢弃)的子任务只会等来重投:不补发就永远落不了定
        when(guard.key(any(), any())).thenReturn("k");
        when(guard.processedOutput("k")).thenReturn("{\"echo\":\"A\",\"length\":1}");
        listener.onMessage(new SubtaskMessage(1L, 10L, "u", "ECHO_BATCH", 0, "{\"text\":\"a\"}"), null);
        verify(processor, never()).process(anyString());
        verify(guard, never()).markProcessed(any(), any());
        org.mockito.ArgumentCaptor<com.agentflow.common.mq.ResultMessage> sent =
                org.mockito.ArgumentCaptor.forClass(com.agentflow.common.mq.ResultMessage.class);
        verify(template).send(eq(Topics.RESULT), eq("10"), sent.capture());
        org.junit.jupiter.api.Assertions.assertTrue(sent.getValue().isSuccess());
        org.junit.jupiter.api.Assertions.assertEquals("{\"echo\":\"A\",\"length\":1}", sent.getValue().getOutputJson());
    }

    @Test
    void processesAndMarksWhenFirstTime() throws Exception {
        when(guard.key(any(), any())).thenReturn("k");
        listener.onMessage(new SubtaskMessage(1L, 10L, "u", "ECHO_BATCH", 0, "{\"text\":\"hi\"}"), null);
        verify(processor).process("{\"text\":\"hi\"}");
        verify(template).send(eq(Topics.RESULT), eq("10"), any());
        verify(guard).markProcessed("k", "{\"echo\":\"HI\",\"length\":2}");
        // 崩溃安全:必须先发 RESULT 再置幂等键(见 SubtaskListener 注释)
        InOrder inOrder = inOrder(template, guard);
        inOrder.verify(template).send(eq(Topics.RESULT), eq("10"), any());
        inOrder.verify(guard).markProcessed(eq("k"), anyString());
    }
}
