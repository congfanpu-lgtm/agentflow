package com.agentflow.worker.llm;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.ArrayList;
import java.util.List;

/**
 * LLM 网关配置:后端选择 + 每模型的限流桶容量/补充速率/计价。
 * 一个模型一个令牌桶 + 一份计价 → 支撑"多模型路由 + 差异化限流/成本"。
 */
@Getter
@Setter
@ConfigurationProperties("llm")
public class LlmModelProperties {
    /** mock(默认)/ openai。 */
    private String provider = "mock";
    /** 请求未指定 model 时用的默认模型名。 */
    private String defaultModel = "mock-small";
    private List<Model> models = new ArrayList<>();

    public Model model(String name) {
        String target = (name == null || name.isBlank()) ? defaultModel : name;
        for (Model m : models) {
            if (m.getName().equals(target)) {
                return m;
            }
        }
        throw new IllegalArgumentException("未配置的 LLM 模型:" + target);
    }

    @Getter
    @Setter
    public static class Model {
        private String name;
        /** 令牌桶容量(应对突发)。 */
        private int capacity = 10;
        /** 稳态补充速率(令牌/秒 ≈ 该模型允许的 QPS)。 */
        private double refillPerSec = 5.0;
        /** 每 1k token 计价(USD),用于成本估算。 */
        private double pricePer1kTokens = 0.0;
    }
}
