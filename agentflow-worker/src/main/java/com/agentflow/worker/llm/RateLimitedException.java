package com.agentflow.worker.llm;

/** 令牌桶耗尽:当前无可用令牌。worker 侧当作普通失败交 W3 阶梯重试(限流是暂时的)。 */
public class RateLimitedException extends RuntimeException {
    public RateLimitedException(String model) {
        super("LLM 限流:model=" + model + " 令牌桶暂无可用令牌");
    }
}
