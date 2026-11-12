package com.agentflow.worker.llm;

import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

/**
 * 所有 LLM 调用的唯一出口(卖点3)。业务代码只注入本 bean,拿不到底层 {@link LlmClient}——
 * 没有旁路(代码层 enforce,对应面经"所有副作用/调用经同一套 policy")。
 *
 * <p>职责:① 路由(按 model 选桶 + 计价)② 限流(Redis+Lua 令牌桶;没令牌先排队等,等太久才抛
 * {@link RateLimitedException} 交 W3 重试)③ 调用 ④ token 记账。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class LlmGateway {

    private final LlmModelProperties props;
    private final RateLimiter rateLimiter;
    private final TokenAccountant accountant;
    /** 仅一个实现处于激活态(@ConditionalOnProperty:mock 默认 / openai 开关)。 */
    private final LlmClient client;

    public ChatResponse chat(ChatRequest request) {
        LlmModelProperties.Model m = props.model(request.getModel());
        request.setModel(m.getName());  // 归一(把默认模型名回填,记账/响应一致)

        awaitToken(m);
        ChatResponse resp = client.chat(request);
        accountant.record(m.getName(), resp.getUsage(), m.getPricePer1kTokens());
        return resp;
    }

    /**
     * 拿不到令牌就排队等,最多等 llm.max-wait-ms,超过才当失败。一被限流就失败的话会走重试阶梯:
     * 每被限流一次烧掉一次业务重试,过载时子任务成片进死信(压测:请求量是配额的 4 倍时见 docs/benchmarks.md)。
     * 等待期间这个消费线程不取新消息,多出来的任务留在 Kafka 里排队。
     */
    private void awaitToken(LlmModelProperties.Model m) {
        long deadline = System.currentTimeMillis() + props.getMaxWaitMs();
        // 每隔大约"补一个令牌的时间"再试一次,上下浮动一半,免得等待的线程同时醒来
        long tick = m.getRefillPerSec() > 0 ? (long) (1000 / m.getRefillPerSec()) : 1000;
        tick = Math.max(20, Math.min(tick, 1000));
        while (!rateLimiter.tryAcquire(m.getName(), m.getCapacity(), m.getRefillPerSec())) {
            long left = deadline - System.currentTimeMillis();
            if (left <= 0) {
                throw new RateLimitedException(m.getName());
            }
            try {
                Thread.sleep(Math.min(left, ThreadLocalRandom.current().nextLong(tick / 2, tick * 3 / 2 + 1)));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RateLimitedException(m.getName());
            }
        }
    }
}
