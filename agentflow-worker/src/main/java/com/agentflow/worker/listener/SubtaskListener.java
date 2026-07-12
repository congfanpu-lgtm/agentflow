package com.agentflow.worker.listener;

import com.agentflow.common.mq.ResultMessage;
import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.common.mq.Topics;
import com.agentflow.worker.idempotency.IdempotencyGuard;
import com.agentflow.worker.processor.EchoProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class SubtaskListener {

    private final EchoProcessor processor;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final IdempotencyGuard guard;

    @KafkaListener(topics = Topics.SUBTASK)
    public void onMessage(SubtaskMessage msg) {
        String idemKey = guard.key(msg.getSubtaskUuid(), msg.getInputJson());
        if (guard.alreadyProcessed(idemKey)) {
            log.info("幂等命中,跳过重复处理 subtaskId={}", msg.getSubtaskId());
            return;
        }
        ResultMessage result;
        try {
            String output = processor.process(msg.getInputJson());
            guard.markProcessed(idemKey);   // 仅成功置键
            result = new ResultMessage(msg.getTaskId(), msg.getSubtaskId(), true, output, null);
        } catch (Exception e) {
            log.error("子任务处理失败 subtaskId={}", msg.getSubtaskId(), e);
            result = new ResultMessage(msg.getTaskId(), msg.getSubtaskId(), false, null, e.getMessage());
        }
        kafkaTemplate.send(Topics.RESULT, String.valueOf(msg.getSubtaskId()), result);
    }
}
