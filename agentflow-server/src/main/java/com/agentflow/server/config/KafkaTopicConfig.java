package com.agentflow.server.config;

import com.agentflow.common.mq.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

/** 启动时经 KafkaAdmin 自动创建 topic(幂等:已存在则跳过);外加消费失败的重试策略。 */
@Configuration
public class KafkaTopicConfig {

    /**
     * 监听器抛异常(结果落库时 MySQL 不通、写 trace 时 Mongo 不通)时怎么重试。Spring Kafka 默认立刻连试 10 次就丢弃这条消息,
     * 依赖一抖消息就没了。改成退避重试:1s 起翻倍、30s 封顶、累计 10 分钟,依赖恢复前不丢。
     * 设上限是为了不让一条必然失败的消息把分区永久堵死;超过上限被放弃的,由超时重投 + worker 补发结果找回。
     */
    @Bean
    public CommonErrorHandler kafkaErrorHandler() {
        ExponentialBackOff backOff = new ExponentialBackOff(1000, 2);
        backOff.setMaxInterval(30_000);
        backOff.setMaxElapsedTime(600_000);
        return new DefaultErrorHandler(backOff);
    }

    /**
     * SUBTASK 分区数 = 全集群同时在处理的子任务上限(每个分区同一时刻只处理一条)。
     * 已有的 topic 启动时会被自动扩到这个数;分区只能加不能减。
     */
    @Bean
    public NewTopic subtaskTopic(@Value("${agentflow.subtask-partitions:8}") int partitions) {
        return TopicBuilder.name(Topics.SUBTASK).partitions(partitions).replicas(1).build();
    }

    @Bean
    public NewTopic resultTopic() {
        return TopicBuilder.name(Topics.RESULT).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic retry5sTopic() {
        return TopicBuilder.name(Topics.RETRY_5S).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic retry30sTopic() {
        return TopicBuilder.name(Topics.RETRY_30S).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic retry5mTopic() {
        return TopicBuilder.name(Topics.RETRY_5M).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic dlqTopic() {
        return TopicBuilder.name(Topics.DLQ).partitions(3).replicas(1).build();
    }
}
