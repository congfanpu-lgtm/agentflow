package com.agentflow.worker.retry;

import com.agentflow.common.mq.ResultMessage;
import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.common.mq.Topics;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class RetryRouterTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, Object> template = mock(KafkaTemplate.class);
    private final RetryProperties props = new RetryProperties();
    private final RetryRouter router = new RetryRouter(template, props);

    private final SubtaskMessage msg =
            new SubtaskMessage(1L, 10L, "u", "ECHO_BATCH", 0, "{\"text\":\"a\"}");

    @Test
    void attempt0GoesToRetry5s() {
        router.route(msg, 0, "boom");
        ArgumentCaptor<ProducerRecord<String, Object>> cap = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(cap.capture());
        assertEquals(Topics.RETRY_5S, cap.getValue().topic());
        assertNotNull(cap.getValue().headers().lastHeader(Topics.NOT_BEFORE_HEADER));
    }

    @Test
    void attempt2GoesToRetry5m() {
        router.route(msg, 2, "boom");
        ArgumentCaptor<ProducerRecord<String, Object>> cap = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(cap.capture());
        assertEquals(Topics.RETRY_5M, cap.getValue().topic());
    }

    @Test
    void attempt3ExhaustsToDlqAndSendsFailedResult() {
        router.route(msg, 3, "boom");
        // 一条进 DLQ(ProducerRecord),一条失败 ResultMessage 进 RESULT(topic,key,value)
        verify(template).send(argThat((ProducerRecord<String, Object> r) -> Topics.DLQ.equals(r.topic())));
        ArgumentCaptor<Object> resultCap = ArgumentCaptor.forClass(Object.class);
        verify(template).send(eq(Topics.RESULT), eq("10"), resultCap.capture());
        ResultMessage rm = (ResultMessage) resultCap.getValue();
        assertFalse(rm.isSuccess());
        assertEquals("boom", rm.getErrorMsg());
    }
}
