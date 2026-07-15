package com.agentflow.worker.processor;

import com.agentflow.common.llm.ChatMessage;
import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import com.agentflow.worker.context.AgentContext;
import com.agentflow.worker.llm.LlmGateway;
import com.agentflow.worker.rag.Hit;
import com.agentflow.worker.rag.RagClient;
import com.agentflow.worker.rag.RagProperties;
import com.agentflow.worker.skill.SchemaValidator;
import com.agentflow.worker.skill.Skill;
import com.agentflow.worker.skill.SkillRegistry;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.regex.Pattern;

/**
 * RESEARCH_BATCH 的多角色 Agent(Runtime 角色流水):
 * router(意图判断是否需要检索)→ retriever(命中才 RAG 检索)→ AgentContext 注入资料 →
 * summarizer(LLM 汇总)→ skill schema 校验。跨子任务的聚合(汇总)由 TaskFinalizer 完成。
 *
 * <p>意图驱动(面经"agent 自主决定是否检索"):检索是有成本的旁路工具,由意图门控——
 * 不是每条都查。判断用轻量启发式(疑问/研究类关键词);LLM 判意图列 backlog(避免每步多一次调用)。
 * 检索只读、不产生副作用,故不经 Policy。
 */
@Component
@RequiredArgsConstructor
public class ResearchProcessor implements SubtaskProcessor {

    private static final String INTENT = "research";
    private static final ObjectMapper objectMapper = new ObjectMapper();
    // 需要检索的信号:问句 / 研究类动词(中英)
    private static final Pattern NEEDS_RAG = Pattern.compile(
            "[?？]|如何|怎么|什么|为什么|对比|比较|区别|根据|资料|介绍|"
            + "how|what|why|which|compare|explain|difference|describe",
            Pattern.CASE_INSENSITIVE);

    private final RagClient ragClient;
    private final RagProperties ragProperties;
    private final LlmGateway gateway;
    private final AgentContext agentContext;
    private final SkillRegistry skillRegistry;
    private final SchemaValidator schemaValidator;

    @Override
    public String type() {
        return "RESEARCH_BATCH";
    }

    /** 意图门控:是否需要外部知识检索。 */
    boolean needsRetrieval(String question) {
        return question != null && NEEDS_RAG.matcher(question).find();
    }

    @Override
    public String process(String inputJson) throws Exception {
        JsonNode input = objectMapper.readTree(inputJson);
        String question = input.path("question").asText("");
        if (question.isBlank()) {
            throw new IllegalArgumentException("input.question 缺失");
        }
        String model = input.hasNonNull("model") ? input.get("model").asText() : null;

        boolean usedRag = needsRetrieval(question);
        List<Hit> hits = usedRag ? ragClient.search(question, ragProperties.getTopK()) : List.of();

        // 把检索资料注入用户输入;AgentContext 会按 token 预算裁剪超长资料(复用 W5-6 防膨胀)
        StringBuilder userInput = new StringBuilder(question);
        if (!hits.isEmpty()) {
            userInput.append("\n\n[资料]");
            for (Hit h : hits) {
                userInput.append("\n- ").append(h.text());
            }
        }

        Skill skill = skillRegistry.selectBest(INTENT)
                .orElseThrow(() -> new IllegalStateException("无命中 skill:" + INTENT));
        List<ChatMessage> messages = agentContext.build(skill.systemPrompt(), userInput.toString(), List.of());
        ChatResponse resp = gateway.chat(new ChatRequest(model, messages, 256));

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

        ObjectNode out = objectMapper.createObjectNode();
        out.put("answer", modelOut.path("summary").asText());
        out.put("usedRag", usedRag);
        ArrayNode retrieved = out.putArray("retrieved");
        for (Hit h : hits) {
            retrieved.add(h.id());
        }
        out.put("model", resp.getModel());
        out.put("promptTokens", resp.getUsage().getPromptTokens());
        out.put("completionTokens", resp.getUsage().getCompletionTokens());
        return objectMapper.writeValueAsString(out);
    }
}
