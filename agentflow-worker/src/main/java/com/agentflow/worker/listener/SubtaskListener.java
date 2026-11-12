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

    // 一个消费线程同一时刻只处理一条(处理完才提交位移),所以 线程数 × worker 数 就是同时在跑的子任务数,上限是分区数
    @KafkaListener(topics = Topics.SUBTASK, concurrency = "${agentflow.worker.subtask-concurrency:4}")
    public void onMessage(SubtaskMessage msg,
                          @Header(name = Topics.RETRY_ATTEMPT_HEADER, required = false) byte[] attemptHeader) {
        int attempt = attemptHeader == null ? 0
                : Integer.parseInt(new String(attemptHeader, StandardCharsets.UTF_8));
        traceEmitter.emit(String.valueOf(msg.getTaskId()), msg.getTaskId(), msg.getSubtaskId(),
                TraceStage.WORKER_RECEIVED, null, "attempt=" + attempt);
        String idemKey = guard.key(msg.getSubtaskUuid(), msg.getInputJson());
        String done = guard.processedOutput(idemKey);
        if (done != null) {
            // 活干过了,但结果不一定送到:发送失败、server 处理时出错被丢弃,都会让子任务停在 DISPATCHED,
            // 等来的只有超时重投。这里只跳过不补发的话,它会一直卡到重投耗尽、被误判失败。
            // 多发的结果由 server 端子任务 CAS 丢弃。
            log.info("幂等命中,不重复处理,补发结果 subtaskId={}", msg.getSubtaskId());
            kafkaTemplate.send(Topics.RESULT, String.valueOf(msg.getSubtaskId()),
                    new ResultMessage(msg.getTaskId(), msg.getSubtaskId(), true, done, null));
            traceEmitter.emit(String.valueOf(msg.getTaskId()), msg.getTaskId(), msg.getSubtaskId(),
                    TraceStage.PROCESSED, "SKIPPED_IDEMPOTENT", null);
            return;
        }
        try {
            SubtaskProcessor processor = processors.get(msg.getType());
            if (processor == null) {
                throw new IllegalArgumentException("未知任务类型:" + msg.getType());
            }
            String output = processor.process(msg);
            traceEmitter.emit(String.valueOf(msg.getTaskId()), msg.getTaskId(), msg.getSubtaskId(),
                    TraceStage.PROCESSED, "OK", null);
            // 先发 RESULT,再存幂等键:崩在两者之间,重投只会多干一遍(server 端 CAS 丢弃多余结果);
            // 反过来先存键再崩在发送前,就得等超时重投来触发上面的补发,白等一个超时周期。
            kafkaTemplate.send(Topics.RESULT, String.valueOf(msg.getSubtaskId()),
                    new ResultMessage(msg.getTaskId(), msg.getSubtaskId(), true, output, null));
            traceEmitter.emit(String.valueOf(msg.getTaskId()), msg.getTaskId(), msg.getSubtaskId(),
                    TraceStage.RESULT_SENT, "true", null);
            try {
                guard.markProcessed(idemKey, output);
            } catch (RuntimeException e) {
                // 结果已经发出。幂等键没写上,最多是以后重复投递时再干一遍;交给重试路由则一定会再干一遍。
                log.warn("幂等键写入失败(结果已发出) subtaskId={}", msg.getSubtaskId(), e);
            }
        } catch (Exception e) {
            log.warn("子任务处理失败 subtaskId={} attempt={},交重试路由", msg.getSubtaskId(), attempt, e);
            traceEmitter.emit(String.valueOf(msg.getTaskId()), msg.getTaskId(), msg.getSubtaskId(),
                    TraceStage.PROCESSED, "FAILED", e.getMessage());
            retryRouter.route(msg, attempt, e.getMessage());
        }
    }
}
