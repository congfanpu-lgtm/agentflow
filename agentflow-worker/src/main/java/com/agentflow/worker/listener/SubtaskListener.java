package com.agentflow.worker.listener;

import com.agentflow.common.mq.ResultMessage;
import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.common.mq.Topics;
import com.agentflow.worker.processor.EchoProcessor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/**
 * Worker 消费入口:处理成功/失败都回传 ResultMessage,由 server 决定状态。
 * 处理异常不上抛(W1-2 无重试;W3-4 起失败上抛触发 Kafka 重投+幂等)。
 * groupId 走 application.yml 的默认(agentflow-worker-consumer);多 Worker 同组 = 分区级并行。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SubtaskListener {

    private final EchoProcessor processor;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = Topics.SUBTASK)
    public void onMessage(SubtaskMessage msg) {
        log.info("收到子任务 taskId={} subtaskId={} seq={}",
                msg.getTaskId(), msg.getSubtaskId(), msg.getSeq());
        ResultMessage result;
        try {
            String output = processor.process(msg.getInputJson());
            result = new ResultMessage(msg.getTaskId(), msg.getSubtaskId(), true, output, null);
        } catch (Exception e) {
            log.error("子任务处理失败 subtaskId={}", msg.getSubtaskId(), e);
            result = new ResultMessage(msg.getTaskId(), msg.getSubtaskId(), false, null,
                    e.getMessage());
        }
        kafkaTemplate.send(Topics.RESULT, String.valueOf(msg.getSubtaskId()), result);
    }
}
