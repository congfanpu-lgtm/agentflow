package com.agentflow.worker.processor;

import com.agentflow.common.llm.ChatMessage;
import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import com.agentflow.worker.agent.Prompts;
import com.agentflow.worker.agent.TextUtil;
import com.agentflow.worker.agent.Usage;
import com.agentflow.worker.llm.LlmGateway;
import com.agentflow.worker.llm.LlmModelProperties;
import com.agentflow.worker.skill.SchemaValidator;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * RESEARCH_AGENT 阶段①:先分流,再拆题。
 * 答案类(kind=answer)不拆,原问题整个作为唯一的子问题;说明类(kind=report)由 LLM 拆成子问题,
 * 输出过 schema 校验 + 去重 + 截断到 4 个;空 → 失败重试。分流输出看不懂时按说明类处理(原来的流程)。
 */
@Component
@RequiredArgsConstructor
public class PlanProcessor implements SubtaskProcessor {

    static final int MAX_SUB_QUESTIONS = 4;
    private static final ObjectMapper OM = new ObjectMapper();

    private final LlmGateway gateway;
    private final LlmModelProperties props;
    private final SchemaValidator validator;

    @Override
    public String type() {
        return "PLAN";
    }

    @Override
    public String process(String inputJson) throws Exception {
        JsonNode in = OM.readTree(inputJson);
        String question = in.path("question").asText("").trim();
        if (question.isEmpty()) {
            throw new IllegalArgumentException("input.question 缺失");
        }
        String model = in.hasNonNull("model") ? in.get("model").asText() : null;
        ChatResponse routed = gateway.chat(new ChatRequest(model, List.of(
                new ChatMessage("system", Prompts.system(Prompts.ROUTER, question)), new ChatMessage("user", question),
                new ChatMessage("user", Prompts.ROUTER_REMINDER)), 60));
        Usage usage = Usage.of(routed, props);
        ObjectNode out = OM.createObjectNode();
        if (isAnswer(routed.getContent())) {
            out.put("kind", "answer").putArray("subQuestions").add(question);
            out.set("usage", OM.valueToTree(usage));
            return out.toString();
        }

        ChatResponse resp = gateway.chat(new ChatRequest(model, List.of(
                new ChatMessage("system", Prompts.system(Prompts.PLANNER, question)), new ChatMessage("user", question),
                new ChatMessage("user", Prompts.PLANNER_REMINDER)), 400));
        usage.add(Usage.of(resp, props));

        JsonNode plan = TextUtil.parseObject(resp.getContent());
        List<String> violations = validator.validate(plan, Map.of("subQuestions", "array"));
        if (!violations.isEmpty()) {
            throw new IllegalStateException("拆题输出不合 schema:" + violations);
        }
        Set<String> subs = new LinkedHashSet<>();
        for (JsonNode n : plan.get("subQuestions")) {
            if (n.isTextual() && !n.asText().isBlank()) {
                subs.add(n.asText().trim());
            }
        }
        if (subs.isEmpty()) {
            throw new IllegalStateException("拆题结果为空");
        }
        ArrayNode arr = out.put("kind", "report").putArray("subQuestions");
        subs.stream().limit(MAX_SUB_QUESTIONS).forEach(arr::add);
        out.set("usage", OM.valueToTree(usage));
        return out.toString();
    }

    private static boolean isAnswer(String content) {
        try {
            return "answer".equals(TextUtil.parseObject(content).path("kind").asText());
        } catch (IllegalStateException e) {
            return false;
        }
    }
}
