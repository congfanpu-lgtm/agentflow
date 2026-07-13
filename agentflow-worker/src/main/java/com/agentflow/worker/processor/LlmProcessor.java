package com.agentflow.worker.processor;

import com.agentflow.common.llm.ChatMessage;
import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import com.agentflow.worker.context.AgentContext;
import com.agentflow.worker.llm.LlmGateway;
import com.agentflow.worker.skill.SchemaValidator;
import com.agentflow.worker.skill.Skill;
import com.agentflow.worker.skill.SkillRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * LLM_BATCH 的真 Agent(Runtime "想和做"):AgentContext 组 prompt → LlmGateway 调模型
 * (限流/记账在网关)→ 按选中的 Skill 的 schema 校验模型输出,不合规则抛(交 W3 重试)。
 * 副作用一律回 Harness 经 Policy——本处理器不产生任何外部副作用。
 */
@Component
@RequiredArgsConstructor
public class LlmProcessor implements SubtaskProcessor {

    private static final String DEFAULT_INTENT = "summarize";

    private final LlmGateway gateway;
    private final AgentContext agentContext;
    private final SkillRegistry skillRegistry;
    private final SchemaValidator schemaValidator;
    private final ObjectMapper objectMapper;

    @Override
    public String type() {
        return "LLM_BATCH";
    }

    @Override
    public String process(String inputJson) throws Exception {
        JsonNode input = objectMapper.readTree(inputJson);
        String text = input.path("text").asText("");
        if (text.isBlank()) {
            throw new IllegalArgumentException("input.text 缺失");
        }
        String model = input.hasNonNull("model") ? input.get("model").asText() : null;

        Skill skill = skillRegistry.selectBest(DEFAULT_INTENT)
                .orElseThrow(() -> new IllegalStateException("无命中 skill:" + DEFAULT_INTENT));

        List<ChatMessage> messages = agentContext.build(skill.systemPrompt(), text, List.of());
        ChatResponse resp = gateway.chat(new ChatRequest(model, messages, 256));

        // 解析并校验模型输出(不让模型自由发挥):非 JSON 或不合 schema → 失败 → 重试
        JsonNode modelOut;
        try {
            modelOut = objectMapper.readTree(resp.getContent());
        } catch (Exception e) {
            throw new IllegalStateException("LLM 输出非 JSON,不合 schema:" + resp.getContent());
        }
        List<String> violations = schemaValidator.validate(modelOut, skill.outputSchema());
        if (!violations.isEmpty()) {
            throw new IllegalStateException("LLM 输出不合 " + skill.name() + " schema:" + violations);
        }

        ObjectNode out = (ObjectNode) modelOut;
        out.put("skill", skill.name());
        out.put("model", resp.getModel());
        out.put("promptTokens", resp.getUsage().getPromptTokens());
        out.put("completionTokens", resp.getUsage().getCompletionTokens());
        return objectMapper.writeValueAsString(out);
    }
}
