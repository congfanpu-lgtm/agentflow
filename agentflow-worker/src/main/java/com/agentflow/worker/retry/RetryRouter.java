package com.agentflow.worker.retry;

import com.agentflow.common.mq.ResultMessage;
import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.common.mq.Topics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/** 失败路由:attempt 0→RETRY_5S,1→RETRY_30S,2→RETRY_5M,>=3→DLQ+回传失败结果。 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RetryRouter {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RetryProperties props;

    public void route(SubtaskMessage msg, int attempt, String errorMsg) {
        String key = String.valueOf(msg.getSubtaskId());
        if (attempt >= RetryProperties.MAX_ATTEMPTS) {
            kafkaTemplate.send(new ProducerRecord<>(Topics.DLQ, key, msg));
            kafkaTemplate.send(Topics.RESULT, key,
                    new ResultMessage(msg.getTaskId(), msg.getSubtaskId(), false, null, errorMsg));
            log.error("子任务重试耗尽,进 DLQ subtaskId={}", msg.getSubtaskId());
            return;
        }
        String topic;
        long delayMs;
        switch (attempt) {
            case 0 -> { topic = Topics.RETRY_5S;  delayMs = props.getDelay5s(); }
            case 1 -> { topic = Topics.RETRY_30S; delayMs = props.getDelay30s(); }
            default -> { topic = Topics.RETRY_5M; delayMs = props.getDelay5m(); }
        }
        long notBefore = System.currentTimeMillis() + delayMs;
        ProducerRecord<String, Object> rec = new ProducerRecord<>(topic, key, msg);
        // 关键:把当前 attempt 一并带上,延迟消费者据此 +1 后重投主 topic(否则档位无法递进)
        rec.headers().add(new RecordHeader(Topics.RETRY_ATTEMPT_HEADER,
                String.valueOf(attempt).getBytes(StandardCharsets.UTF_8)));
        rec.headers().add(new RecordHeader(Topics.NOT_BEFORE_HEADER,
                String.valueOf(notBefore).getBytes(StandardCharsets.UTF_8)));
        kafkaTemplate.send(rec);
        log.info("子任务转重试 subtaskId={} attempt={} topic={} delayMs={}",
                msg.getSubtaskId(), attempt, topic, delayMs);
    }
}
