package com.agentflow.worker.llm;

import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;

/**
 * LLM 后端抽象。默认实现 {@link MockLlmClient}(确定性、不烧钱、无需 key,压测/单测都用它);
 * 真实模型 {@link OpenAiLlmClient} 由 {@code llm.provider=openai} 开关启用。
 * 网关({@link LlmGateway})按 provider 选实现;业务代码拿不到 client 本身,只能经网关。
 */
public interface LlmClient {
    /** provider 名,与 {@code llm.provider} 配置匹配。 */
    String provider();

    ChatResponse chat(ChatRequest request);
}
