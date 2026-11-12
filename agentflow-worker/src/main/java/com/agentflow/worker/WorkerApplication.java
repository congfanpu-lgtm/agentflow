package com.agentflow.worker;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.ExponentialBackOff;

@SpringBootApplication
@ConfigurationPropertiesScan
public class WorkerApplication {
    public static void main(String[] args) {
        new SpringApplicationBuilder(WorkerApplication.class)
                .web(WebApplicationType.NONE)
                .run(args);
    }

    /**
     * 监听器抛异常(查幂等键时 Redis 不通)时怎么重试。Spring Kafka 默认立刻连试 10 次就丢弃这条消息,
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
}
