package com.agentflow.server.trace;

import com.agentflow.common.mq.Topics;
import com.agentflow.common.trace.TraceEvent;
import com.agentflow.common.trace.TraceStage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** 统一 trace 出口:每个状态变更 emit 到 Kafka TRACE。emit 失败不拖垮业务(只 warn)。 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TraceEmitter {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void emit(String traceId, Long taskId, Long subtaskId,
                     TraceStage stage, String status, String detail) {
        try {
            TraceEvent ev = new TraceEvent(traceId, taskId, subtaskId,
                    stage.name(), status, detail, System.currentTimeMillis());
            kafkaTemplate.send(Topics.TRACE, traceId, ev);
        } catch (Exception e) {
            log.warn("trace emit 失败 traceId={} stage={}", traceId, stage, e);
        }
    }
}
