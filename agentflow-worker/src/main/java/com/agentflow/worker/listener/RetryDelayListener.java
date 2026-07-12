package com.agentflow.worker.listener;

import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.common.mq.Topics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/** 重试延迟消费者:读 x-not-before sleep 到点,重投主 topic,attempt+1。
 *  max.poll.records=1 + max.poll.interval.ms=600000 保证单条 sleep 不触发 rebalance。 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RetryDelayListener {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = {Topics.RETRY_5S, Topics.RETRY_30S, Topics.RETRY_5M})
    public void onRetry(SubtaskMessage msg,
                        @Header(name = Topics.NOT_BEFORE_HEADER, required = false) byte[] notBeforeH,
                        @Header(name = Topics.RETRY_ATTEMPT_HEADER, required = false) byte[] attemptH)
            throws InterruptedException {
        int attempt = attemptH == null ? 0 : Integer.parseInt(new String(attemptH, StandardCharsets.UTF_8));
        long notBefore = notBeforeH == null ? 0 : Long.parseLong(new String(notBeforeH, StandardCharsets.UTF_8));
        long waitMs = notBefore - System.currentTimeMillis();
        if (waitMs > 0) Thread.sleep(waitMs);
        ProducerRecord<String, Object> rec = new ProducerRecord<>(
                Topics.SUBTASK, String.valueOf(msg.getSubtaskId()), msg);
        rec.headers().add(new RecordHeader(Topics.RETRY_ATTEMPT_HEADER,
                String.valueOf(attempt + 1).getBytes(StandardCharsets.UTF_8)));
        kafkaTemplate.send(rec);
        log.info("重试到点,重投主 topic subtaskId={} 新 attempt={}", msg.getSubtaskId(), attempt + 1);
    }
}
