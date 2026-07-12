package com.agentflow.server.config;

import com.agentflow.common.mq.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/** 启动时经 KafkaAdmin 自动创建 topic(幂等:已存在则跳过)。 */
@Configuration
public class KafkaTopicConfig {

    @Bean
    public NewTopic subtaskTopic() {
        return TopicBuilder.name(Topics.SUBTASK).partitions(3).replicas(1).build();
    }

    @Bean
    public NewTopic resultTopic() {
        return TopicBuilder.name(Topics.RESULT).partitions(3).replicas(1).build();
    }
}
