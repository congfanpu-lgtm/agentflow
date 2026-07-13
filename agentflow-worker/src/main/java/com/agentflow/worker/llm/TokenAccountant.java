package com.agentflow.worker.llm;

import com.agentflow.common.llm.TokenUsage;
import lombok.RequiredArgsConstructor;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

/**
 * Token 记账:每次 LLM 调用把 prompt/completion token 数与估算成本累加到 Redis(原子 INCRBY)。
 * 成本以微美元(long)累加,避免浮点累加漂移。server 的 /llm/usage 直接按同一 key 规约读回。
 *
 * <p>key 规约(server 侧按此 SCAN + 读):
 * {@code agentflow:tokens:<model>:prompt} · {@code :completion} · {@code :costmicro}。
 */
@Component
@RequiredArgsConstructor
public class TokenAccountant {

    public static final String PREFIX = "agentflow:tokens:";

    private final RedissonClient redisson;

    public void record(String model, TokenUsage usage, double pricePer1kTokens) {
        if (usage == null) {
            return;
        }
        String base = PREFIX + model;
        redisson.getAtomicLong(base + ":prompt").addAndGet(usage.getPromptTokens());
        redisson.getAtomicLong(base + ":completion").addAndGet(usage.getCompletionTokens());
        long costMicro = Math.round(usage.total() / 1000.0 * pricePer1kTokens * 1_000_000);
        redisson.getAtomicLong(base + ":costmicro").addAndGet(costMicro);
    }

    /** 单模型累计(worker 侧自查用)。 */
    public long promptTokens(String model) {
        return redisson.getAtomicLong(PREFIX + model + ":prompt").get();
    }

    public long completionTokens(String model) {
        return redisson.getAtomicLong(PREFIX + model + ":completion").get();
    }
}
