package com.agentflow.worker.processor;

import com.agentflow.common.llm.ChatMessage;
import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import com.agentflow.worker.agent.Prompts;
import com.agentflow.worker.agent.TextUtil;
import com.agentflow.worker.agent.Usage;
import com.agentflow.worker.llm.LlmGateway;
import com.agentflow.worker.llm.LlmModelProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * RESEARCH_AGENT 反思阶段:审阅各 agent 的结论,找出回答原问题还缺什么,输出最多 2 个追问
 * (服务端据此再派 agent,只做一轮)。输出解析失败按"无缺口"处理:反思是增益环节,不为格式问题烧重试。
 */
@Component
@RequiredArgsConstructor
public class ReflectProcessor implements SubtaskProcessor {

    static final int MAX_GAPS = 2;
    private static final ObjectMapper OM = new ObjectMapper();

    private final LlmGateway gateway;
    private final LlmModelProperties props;

    @Override
    public String type() {
        return "REFLECT";
    }

    @Override
    public String process(String inputJson) throws Exception {
        JsonNode in = OM.readTree(inputJson);
        String question = in.path("question").asText();
        String model = in.hasNonNull("model") ? in.get("model").asText() : null;
        StringBuilder user = new StringBuilder("原问题:").append(question).append("\n\n各子问题结论:\n");
        for (JsonNode f : in.path("findings")) {
            user.append("### ").append(f.path("subQuestion").asText()).append('\n')
                    .append(f.path("answer").asText()).append("\n来源数:").append(f.path("sources").size()).append("\n\n");
        }
        ChatResponse resp = gateway.chat(new ChatRequest(model, List.of(
                new ChatMessage("system", Prompts.system(Prompts.REFLECTOR, question)),
                new ChatMessage("user", user.toString())), 400));

        Set<String> gaps = new LinkedHashSet<>();
        String reason = "";
        try {
            JsonNode r = TextUtil.parseObject(resp.getContent());
            for (JsonNode g : r.path("gaps")) {
                if (g.isTextual() && !g.asText().isBlank()) {
                    gaps.add(g.asText().trim());
                }
            }
            reason = r.path("reason").asText("");
        } catch (IllegalStateException e) {
            reason = "反思输出无法解析,按无缺口处理";
        }
        ObjectNode out = OM.createObjectNode();
        ArrayNode arr = out.putArray("gaps");
        gaps.stream().limit(MAX_GAPS).forEach(arr::add);
        out.put("reason", reason);
        out.set("usage", OM.valueToTree(Usage.of(resp, props)));
        return out.toString();
    }
}
