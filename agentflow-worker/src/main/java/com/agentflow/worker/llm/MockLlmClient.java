package com.agentflow.worker.llm;

import com.agentflow.common.llm.ChatMessage;
import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import com.agentflow.common.llm.TokenUsage;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

/**
 * 默认 LLM 后端:确定性输出、不发网络、无需 API key——压测测调度不烧钱(master 设计文档第 149 行)。
 * 网关的限流/路由/记账对 mock 与真实模型完全一致,mock 只是把"发 HTTP"换成"本地确定性返回"。
 */
@Component
@ConditionalOnProperty(name = "llm.provider", havingValue = "mock", matchIfMissing = true)
public class MockLlmClient implements LlmClient {

    private static final ObjectMapper OM = new ObjectMapper();

    @Override
    public String provider() {
        return "mock";
    }

    @Override
    public ChatResponse chat(ChatRequest request) {
        String lastUser = "";
        int promptChars = 0;
        if (request.getMessages() != null) {
            for (ChatMessage m : request.getMessages()) {
                if (m.getContent() != null) {
                    promptChars += m.getContent().length();
                    if ("user".equals(m.getRole())) {
                        lastUser = m.getContent();
                    }
                }
            }
        }
        // 模拟"守规矩的模型":确定性输出符合 summarize schema 的 JSON——processor 会真的解析+校验它,
        // 校验通过才算成功(schema 约束是代码层 enforce,不是求模型听话)。
        String trimmed = lastUser.length() > 120 ? lastUser.substring(0, 120) : lastUser;
        String content;
        try {
            content = OM.writeValueAsString(
                    OM.createObjectNode().put("summary", "SUMMARY: " + trimmed.trim()));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
        // token 估算:chars/4(见 TokenEstimator 同款启发式);mock 无真实 usage 故本地估。
        TokenUsage usage = new TokenUsage(Math.max(1, promptChars / 4),
                Math.max(1, content.length() / 4));
        return new ChatResponse(content, usage, request.getModel());
    }
}
