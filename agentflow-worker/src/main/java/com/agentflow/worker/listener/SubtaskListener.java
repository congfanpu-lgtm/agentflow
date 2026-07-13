package com.agentflow.worker.listener;

import com.agentflow.common.mq.ResultMessage;
import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.common.mq.Topics;
import com.agentflow.common.trace.TraceStage;
import com.agentflow.worker.idempotency.IdempotencyGuard;
import com.agentflow.worker.processor.SubtaskProcessor;
import com.agentflow.worker.retry.RetryRouter;
import com.agentflow.worker.trace.TraceEmitter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Component
public class SubtaskListener {

    /** type → 处理器(Spring 注入所有 SubtaskProcessor,按 type 建索引:策略模式)。 */
    private final Map<String, SubtaskProcessor> processors;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final IdempotencyGuard guard;
    private final RetryRouter retryRouter;
    private final TraceEmitter traceEmitter;

    public SubtaskListener(List<SubtaskProcessor> processors,
                           KafkaTemplate<String, Object> kafkaTemplate,
                           IdempotencyGuard guard, RetryRouter retryRouter,
                           TraceEmitter traceEmitter) {
        this.processors = processors.stream()
                .collect(Collectors.toMap(SubtaskProcessor::type, Function.identity()));
        this.kafkaTemplate = kafkaTemplate;
        this.guard = guard;
        this.retryRouter = retryRouter;
        this.traceEmitter = traceEmitter;
    }

    @KafkaListener(topics = Topics.SUBTASK)
    public void onMessage(SubtaskMessage msg,
                          @Header(name = Topics.RETRY_ATTEMPT_HEADER, required = false) byte[] attemptHeader) {
        int attempt = attemptHeader == null ? 0
                : Integer.parseInt(new String(attemptHeader, StandardCharsets.UTF_8));
        traceEmitter.emit(String.valueOf(msg.getTaskId()), msg.getTaskId(), msg.getSubtaskId(),
                TraceStage.WORKER_RECEIVED, null, "attempt=" + attempt);
        String idemKey = guard.key(msg.getSubtaskUuid(), msg.getInputJson());
        if (guard.alreadyProcessed(idemKey)) {
            log.info("幂等命中,跳过重复处理 subtaskId={}", msg.getSubtaskId());
            traceEmitter.emit(String.valueOf(msg.getTaskId()), msg.getTaskId(), msg.getSubtaskId(),
                    TraceStage.PROCESSED, "SKIPPED_IDEMPOTENT", null);
            return;
        }
        try {
            SubtaskProcessor processor = processors.get(msg.getType());
            if (processor == null) {
                throw new IllegalArgumentException("未知任务类型:" + msg.getType());
            }
            String output = processor.process(msg.getInputJson());
            traceEmitter.emit(String.valueOf(msg.getTaskId()), msg.getTaskId(), msg.getSubtaskId(),
                    TraceStage.PROCESSED, "OK", null);
            // 先发 RESULT,再标记幂等键:发送更安全——服务端子任务 CAS 已对重复结果去重,
            // 若崩溃发生在“发送成功之后、markProcessed 之前”,重投顶多导致一次无害的重复处理
            // (纯回显 + 服务端幂等丢弃);反之若先 markProcessed 再崩溃在发送前,则结果永久丢失,
            // 且重投会被幂等直接跳过,最终被超时兜底误判为失败——不可接受。
            kafkaTemplate.send(Topics.RESULT, String.valueOf(msg.getSubtaskId()),
                    new ResultMessage(msg.getTaskId(), msg.getSubtaskId(), true, output, null));
            traceEmitter.emit(String.valueOf(msg.getTaskId()), msg.getTaskId(), msg.getSubtaskId(),
                    TraceStage.RESULT_SENT, "true", null);
            guard.markProcessed(idemKey);
        } catch (Exception e) {
            log.warn("子任务处理失败 subtaskId={} attempt={},交重试路由", msg.getSubtaskId(), attempt, e);
            traceEmitter.emit(String.valueOf(msg.getTaskId()), msg.getTaskId(), msg.getSubtaskId(),
                    TraceStage.PROCESSED, "FAILED", e.getMessage());
            retryRouter.route(msg, attempt, e.getMessage());
        }
    }
}
