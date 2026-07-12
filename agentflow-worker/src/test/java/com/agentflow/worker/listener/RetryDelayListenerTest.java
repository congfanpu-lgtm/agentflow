package com.agentflow.worker.listener;

import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.common.mq.Topics;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

class RetryDelayListenerTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, Object> template = mock(KafkaTemplate.class);
    private final RetryDelayListener listener = new RetryDelayListener(template);

    private final SubtaskMessage msg =
            new SubtaskMessage(1L, 10L, "u", "ECHO_BATCH", 0, "{\"text\":\"a\"}");

    private byte[] pastNotBefore() {
        return String.valueOf(System.currentTimeMillis() - 1000).getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void reproducesToMainTopicWithIncrementedAttempt() throws Exception {
        byte[] notBeforeBytes = pastNotBefore();
        byte[] attemptBytes = "1".getBytes(StandardCharsets.UTF_8);

        listener.onRetry(msg, notBeforeBytes, attemptBytes);

        ArgumentCaptor<ProducerRecord<String, Object>> cap = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(cap.capture());
        assertEquals(Topics.SUBTASK, cap.getValue().topic());
        var attemptHeader = cap.getValue().headers().lastHeader(Topics.RETRY_ATTEMPT_HEADER);
        assertNotNull(attemptHeader);
        assertEquals("2", new String(attemptHeader.value(), StandardCharsets.UTF_8));
    }

    @Test
    void defaultsAttemptToZeroWhenHeaderAbsent() throws Exception {
        byte[] notBeforeBytes = pastNotBefore();

        listener.onRetry(msg, notBeforeBytes, null);

        ArgumentCaptor<ProducerRecord<String, Object>> cap = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(cap.capture());
        assertEquals(Topics.SUBTASK, cap.getValue().topic());
        var attemptHeader = cap.getValue().headers().lastHeader(Topics.RETRY_ATTEMPT_HEADER);
        assertNotNull(attemptHeader);
        assertEquals("1", new String(attemptHeader.value(), StandardCharsets.UTF_8));
    }
}
