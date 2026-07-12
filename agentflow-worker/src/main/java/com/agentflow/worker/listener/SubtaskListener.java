package com.agentflow.worker.listener;

import com.agentflow.common.mq.ResultMessage;
import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.common.mq.Topics;
import com.agentflow.worker.idempotency.IdempotencyGuard;
import com.agentflow.worker.processor.EchoProcessor;
import com.agentflow.worker.retry.RetryRouter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Slf4j
@Component
@RequiredArgsConstructor
public class SubtaskListener {

    private final EchoProcessor processor;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final IdempotencyGuard guard;
    private final RetryRouter retryRouter;

    @KafkaListener(topics = Topics.SUBTASK)
    public void onMessage(SubtaskMessage msg,
                          @Header(name = Topics.RETRY_ATTEMPT_HEADER, required = false) byte[] attemptHeader) {
        int attempt = attemptHeader == null ? 0
                : Integer.parseInt(new String(attemptHeader, StandardCharsets.UTF_8));
        String idemKey = guard.key(msg.getSubtaskUuid(), msg.getInputJson());
        if (guard.alreadyProcessed(idemKey)) {
            log.info("幂等命中,跳过重复处理 subtaskId={}", msg.getSubtaskId());
            return;
        }
        try {
            String output = processor.process(msg.getInputJson());
            // 先发 RESULT,再标记幂等键:发送更安全——服务端子任务 CAS 已对重复结果去重,
            // 若崩溃发生在“发送成功之后、markProcessed 之前”,重投顶多导致一次无害的重复处理
            // (纯回显 + 服务端幂等丢弃);反之若先 markProcessed 再崩溃在发送前,则结果永久丢失,
            // 且重投会被幂等直接跳过,最终被超时兜底误判为失败——不可接受。
            kafkaTemplate.send(Topics.RESULT, String.valueOf(msg.getSubtaskId()),
                    new ResultMessage(msg.getTaskId(), msg.getSubtaskId(), true, output, null));
            guard.markProcessed(idemKey);
        } catch (Exception e) {
            log.warn("子任务处理失败 subtaskId={} attempt={},交重试路由", msg.getSubtaskId(), attempt, e);
            retryRouter.route(msg, attempt, e.getMessage());
        }
    }
}
