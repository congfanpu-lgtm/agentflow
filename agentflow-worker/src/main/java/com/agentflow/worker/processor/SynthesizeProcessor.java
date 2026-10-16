package com.agentflow.worker.processor;

import com.agentflow.common.llm.ChatMessage;
import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import com.agentflow.worker.agent.Prompts;
import com.agentflow.worker.agent.Usage;
import com.agentflow.worker.llm.LlmGateway;
import com.agentflow.worker.llm.LlmModelProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * RESEARCH_AGENT 阶段③:把各 agent 的结论合成一篇带 [n] 引用的报告。
 * 来源按 URL 去重后统一编号;报告里超出编号范围的 [n](编造的引用)直接删掉。
 */
@Component
@RequiredArgsConstructor
public class SynthesizeProcessor implements SubtaskProcessor {

    private static final Pattern CITE = Pattern.compile("\\[(\\d{1,3})]");
    private static final ObjectMapper OM = new ObjectMapper();

    private final LlmGateway gateway;
    private final LlmModelProperties props;

    @Override
    public String type() {
        return "SYNTHESIZE";
    }

    @Override
    public String process(String inputJson) throws Exception {
        JsonNode in = OM.readTree(inputJson);
        String question = in.path("question").asText();
        String model = in.hasNonNull("model") ? in.get("model").asText() : null;

        Map<String, String> urlToTitle = new LinkedHashMap<>();
        StringBuilder findings = new StringBuilder();
        for (JsonNode f : in.path("findings")) {
            findings.append("### ").append(f.path("subQuestion").asText()).append('\n')
                    .append(f.path("answer").asText()).append("\n\n");
            for (JsonNode s : f.path("sources")) {
                String url = s.path("url").asText("");
                if (!url.isEmpty()) {
                    urlToTitle.putIfAbsent(url, s.path("title").asText(url));
                }
            }
        }
        List<Map.Entry<String, String>> sources = new ArrayList<>(urlToTitle.entrySet());
        StringBuilder user = new StringBuilder("问题:").append(question).append("\n\n可引用来源:\n");
        for (int i = 0; i < sources.size(); i++) {
            user.append('[').append(i + 1).append("] ").append(sources.get(i).getValue())
                    .append(' ').append(sources.get(i).getKey()).append('\n');
        }
        user.append("\n各子问题结论:\n").append(findings);

        ChatResponse resp = gateway.chat(new ChatRequest(model, List.of(
                new ChatMessage("system", Prompts.SYNTHESIZER), new ChatMessage("user", user.toString())), 1500));
        String report = stripInvalidCitations(resp.getContent(), sources.size());
        if (report.isBlank()) {
            throw new IllegalStateException("合成报告为空");
        }

        ObjectNode out = OM.createObjectNode().put("report", report);
        ArrayNode arr = out.putArray("sources");
        for (int i = 0; i < sources.size(); i++) {
            arr.addObject().put("n", i + 1).put("title", sources.get(i).getValue()).put("url", sources.get(i).getKey());
        }
        out.set("usage", OM.valueToTree(Usage.of(resp, props)));
        return out.toString();
    }

    static String stripInvalidCitations(String text, int max) {
        return CITE.matcher(text == null ? "" : text).replaceAll(m -> {
            int n = Integer.parseInt(m.group(1));
            return n >= 1 && n <= max ? m.group() : "";
        }).trim();
    }
}
