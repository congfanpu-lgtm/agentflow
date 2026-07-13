package com.agentflow.worker.llm;

import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * 所有 LLM 调用的唯一出口(卖点3)。业务代码只注入本 bean,拿不到底层 {@link LlmClient}——
 * 没有旁路(代码层 enforce,对应面经"所有副作用/调用经同一套 policy")。
 *
 * <p>职责:① 路由(按 model 选桶 + 计价)② 限流(Redis+Lua 令牌桶,失败抛
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

        if (!rateLimiter.tryAcquire(m.getName(), m.getCapacity(), m.getRefillPerSec())) {
            throw new RateLimitedException(m.getName());
        }
        ChatResponse resp = client.chat(request);
        accountant.record(m.getName(), resp.getUsage(), m.getPricePer1kTokens());
        return resp;
    }
}
