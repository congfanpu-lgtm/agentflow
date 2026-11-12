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

    // 3 个线程对应 RESULT 的 3 个分区。同一任务的结果会被并发处理,靠任务计数行锁串行化(见 ResultHandleService);
    // 单线程时每秒约 130 条结果就是整个引擎的上限(docs/benchmarks.md)。
    @KafkaListener(topics = Topics.RESULT, concurrency = "${agentflow.result-concurrency:3}")
    public void onMessage(ResultMessage msg) {
        resultHandleService.handle(msg);
    }
}
