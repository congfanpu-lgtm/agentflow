package com.agentflow.server.trace;

import com.agentflow.common.mq.Topics;
import com.agentflow.common.trace.TraceEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** 单一 trace 落库出口:TRACE topic → MongoDB。 */
@Component
@RequiredArgsConstructor
public class TraceConsumer {

    private final TraceEventRepository repository;

    @KafkaListener(topics = Topics.TRACE, groupId = "agentflow-trace-consumer")
    public void onTrace(TraceEvent ev) {
        TraceDocument doc = new TraceDocument();
        doc.setTraceId(ev.getTraceId());
        doc.setTaskId(ev.getTaskId());
        doc.setSubtaskId(ev.getSubtaskId());
        doc.setStage(ev.getStage());
        doc.setStatus(ev.getStatus());
        doc.setDetail(ev.getDetail());
        doc.setTimestamp(ev.getTimestamp());
        repository.save(doc);
    }
}
