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
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * RESEARCH_AGENT 最后阶段:把各 agent 的结论合成一篇带 [n] 引用的报告。
 * 来源按 URL 去重后统一编号;每个来源附 agent 核对过的原文摘录(证据),每条结论标出它依据哪些来源,
 * 写报告的模型据此只给证据支持的句子加引用。报告里超出编号范围的 [n](编造的引用)直接删掉。
 */
@Component
@RequiredArgsConstructor
public class SynthesizeProcessor implements SubtaskProcessor {

    static final int MAX_QUOTES_PER_SOURCE = 2;
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
        Map<String, Set<String>> urlToQuotes = new LinkedHashMap<>();
        for (JsonNode f : in.path("findings")) {
            for (JsonNode s : f.path("sources")) {
                String url = s.path("url").asText("");
                if (!url.isEmpty()) {
                    urlToTitle.putIfAbsent(url, s.path("title").asText(url));
                    Set<String> quotes = urlToQuotes.computeIfAbsent(url, k -> new LinkedHashSet<>());
                    if (!s.path("quote").asText("").isBlank() && quotes.size() < MAX_QUOTES_PER_SOURCE) {
                        quotes.add(s.get("quote").asText().trim());
                    }
                }
            }
        }
        List<Map.Entry<String, String>> sources = new ArrayList<>(urlToTitle.entrySet());
        Map<String, Integer> number = new HashMap<>();
        StringBuilder user = new StringBuilder("问题:").append(question).append("\n\n可引用来源(附原文证据):\n");
        for (int i = 0; i < sources.size(); i++) {
            String url = sources.get(i).getKey();
            number.put(url, i + 1);
            user.append('[').append(i + 1).append("] ").append(sources.get(i).getValue()).append(' ').append(url).append('\n');
            for (String q : urlToQuotes.get(url)) {
                user.append("    证据:\"").append(q).append("\"\n");
            }
        }
        user.append("\n各子问题结论:\n");
        for (JsonNode f : in.path("findings")) {
            user.append("### ").append(f.path("subQuestion").asText()).append('\n').append(f.path("answer").asText());
            Set<Integer> used = new TreeSet<>();
            f.path("sources").forEach(s -> {
                Integer n = number.get(s.path("url").asText(""));
                if (n != null) {
                    used.add(n);
                }
            });
            if (!used.isEmpty()) {
                user.append("\n依据来源:");
                used.forEach(n -> user.append('[').append(n).append(']'));
            }
            user.append("\n\n");
        }

        ChatResponse resp = gateway.chat(new ChatRequest(model, List.of(
                new ChatMessage("system", Prompts.system(Prompts.SYNTHESIZER, question)), new ChatMessage("user", user.toString())), 4000));
        String report = stripInvalidCitations(resp.getContent(), sources.size());
        if (report.isBlank()) {
            throw new IllegalStateException("合成报告为空");
        }

        ObjectNode out = OM.createObjectNode().put("report", report);
        ArrayNode arr = out.putArray("sources");
        for (int i = 0; i < sources.size(); i++) {
            ObjectNode src = arr.addObject().put("n", i + 1).put("title", sources.get(i).getValue())
                    .put("url", sources.get(i).getKey());
            urlToQuotes.get(sources.get(i).getKey()).stream().findFirst().ifPresent(q -> src.put("quote", q));
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
