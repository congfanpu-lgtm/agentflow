package com.agentflow.server.listener;

import com.agentflow.common.mq.ResultMessage;
import com.agentflow.common.mq.Topics;
import com.agentflow.server.service.ResultHandleService;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** 消费 RESULT topic;groupId 继承 yml 的 agentflow-server-result-consumer,仅委托 handle。 */
@Component
@RequiredArgsConstructor
public class ResultListener {

    private final ResultHandleService resultHandleService;

    @KafkaListener(topics = Topics.RESULT)
    public void onMessage(ResultMessage msg) {
        resultHandleService.handle(msg);
    }
}
