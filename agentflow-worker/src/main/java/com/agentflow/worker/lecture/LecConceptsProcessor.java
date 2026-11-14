package com.agentflow.worker.lecture;

import com.agentflow.common.llm.ChatMessage;
import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import com.agentflow.worker.agent.Prompts;
import com.agentflow.worker.agent.TextUtil;
import com.agentflow.worker.agent.Usage;
import com.agentflow.worker.lecture.Segmenter.Segment;
import com.agentflow.worker.llm.LlmGateway;
import com.agentflow.worker.llm.LlmModelProperties;
import com.agentflow.worker.media.MediaClient;
import com.agentflow.worker.processor.SubtaskProcessor;
import com.agentflow.worker.retry.NonRetryableException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * 讲次摄入第 5 步:这一讲的知识点目录,每讲一次模型调用。
 *
 * <p>模型看到的是按时间编号的片段,输出"知识点 + 片段编号";时间由这里按编号查出来。
 * 模型写的任何时间都不会被采用,所以目录里不会出现这一讲里不存在的时间点。
 * 编号越界、没有名字的条目丢弃;一条能用的都没有就算这一步失败(会重试),不存一份空目录。
 */
@Component
@RequiredArgsConstructor
public class LecConceptsProcessor implements SubtaskProcessor {

    static final int MAX_CONCEPTS = 30;
    static final int MAX_TOKENS = 2000;
    private static final int MAX_NAME = 80;
    private static final int MAX_SUMMARY = 200;
    private static final ObjectMapper OM = new ObjectMapper();

    private final MediaClient media;
    private final LlmGateway gateway;
    private final LlmModelProperties props;

    private record Concept(String name, long startMs, String summary) {
    }

    @Override
    public String type() {
        return "LEC_CONCEPTS";
    }

    @Override
    public String process(String inputJson) throws Exception {
        JsonNode in = OM.readTree(inputJson);
        String title = in.path("title").asText("");
        String model = in.hasNonNull("model") ? in.get("model").asText() : null;
        List<Segment> segments = Segmenter.speech(media.transcript(in.path("mediaKey").asText()).path("utterances"));
        if (segments.isEmpty()) {
            throw new NonRetryableException("这一讲没有识别出语音");
        }
        StringBuilder user = new StringBuilder("标题:").append(title);
        for (int i = 0; i < segments.size(); i++) {
            Segment s = segments.get(i);
            user.append('\n').append('[').append(i).append("] ").append(clock(s.startMs())).append(' ').append(s.text());
        }
        ChatResponse resp = gateway.chat(new ChatRequest(model, List.of(
                new ChatMessage("system", Prompts.system(Prompts.CONCEPTS, title + " " + segments.get(0).text())),
                new ChatMessage("user", user.toString())), MAX_TOKENS));

        List<Concept> valid = new ArrayList<>();
        for (JsonNode c : TextUtil.parseObject(resp.getContent()).path("concepts")) {
            String name = c.path("name").asText("").strip();
            JsonNode segment = c.path("segment");
            if (name.isEmpty() || !segment.isIntegralNumber() || segment.asInt() < 0 || segment.asInt() >= segments.size()) {
                continue;
            }
            valid.add(new Concept(cut(name, MAX_NAME), segments.get(segment.asInt()).startMs(),
                    cut(c.path("summary").asText("").strip(), MAX_SUMMARY)));
        }
        valid.sort(Comparator.comparingLong(Concept::startMs));   // 稳定排序:同一时间的保持模型给的顺序
        ObjectNode out = OM.createObjectNode();
        ArrayNode concepts = out.putArray("concepts");
        Set<String> names = new HashSet<>();
        for (Concept c : valid) {
            if (concepts.size() < MAX_CONCEPTS && names.add(c.name())) {   // 同名只留最早的一条
                concepts.addObject().put("name", c.name()).put("startMs", c.startMs()).put("summary", c.summary());
            }
        }
        if (concepts.isEmpty()) {
            throw new IllegalStateException("模型没有给出可用的知识点");
        }
        out.set("usage", OM.valueToTree(Usage.of(resp, props)));
        return out.toString();
    }

    /** 分:秒;分钟数可以超过 59(一讲两小时就是 118:20)。 */
    private static String clock(long ms) {
        return String.format("%02d:%02d", ms / 60_000, ms / 1000 % 60);
    }

    private static String cut(String s, int max) {
        return s.length() > max ? s.substring(0, max) : s;
    }
}
