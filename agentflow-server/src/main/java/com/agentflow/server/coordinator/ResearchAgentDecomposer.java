package com.agentflow.server.coordinator;

import com.agentflow.common.state.SubtaskStatus;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * RESEARCH_AGENT:plan → map → reduce 三阶段。
 * 提交时只建 1 个 PLAN;PLAN 成功 → N 个 AGENT(并行);AGENT 全部落定且至少 1 个成功 → 1 个 SYNTHESIZE。
 * 每个阶段的每个子任务都是普通 Kafka 子任务,复用重试 / DLQ / 超时兜底 / 幂等 / trace。
 */
@Component
@RequiredArgsConstructor
public class ResearchAgentDecomposer implements TaskDecomposer {

    static final int MAX_QUESTION_CHARS = 500;
    static final String PLAN = "PLAN";
    static final String AGENT = "AGENT";
    static final String SYNTHESIZE = "SYNTHESIZE";

    private final ObjectMapper objectMapper;

    @Override
    public String type() {
        return "RESEARCH_AGENT";
    }

    @Override
    public List<SubtaskDef> decompose(JsonNode payload) {
        String question = payload == null ? "" : payload.path("question").asText("").trim();
        if (question.isEmpty()) {
            throw new IllegalArgumentException("payload.question 不能为空");
        }
        if (question.length() > MAX_QUESTION_CHARS) {
            throw new IllegalArgumentException("payload.question 不能超过 " + MAX_QUESTION_CHARS + " 字");
        }
        return List.of(new SubtaskDef(0, input(question, model(payload)).toString(), PLAN));
    }

    @Override
    public List<SubtaskDef> nextStage(TaskEntity task, List<SubtaskEntity> subs) {
        if (subs.stream().anyMatch(s -> SYNTHESIZE.equals(s.getKind()))) {
            return List.of();
        }
        JsonNode payload = json(task.getPayload());
        String model = model(payload);
        List<SubtaskEntity> agents = subs.stream().filter(s -> AGENT.equals(s.getKind())).toList();

        if (agents.isEmpty()) {   // 阶段①刚结束:按拆题结果扇出 agent
            SubtaskEntity plan = subs.stream().filter(s -> PLAN.equals(s.getKind())).findFirst().orElse(null);
            if (plan == null || !SubtaskStatus.COMPLETED.name().equals(plan.getStatus())) {
                return List.of();
            }
            List<SubtaskDef> defs = new ArrayList<>();
            int seq = 1;
            for (JsonNode q : json(plan.getOutput()).path("subQuestions")) {
                defs.add(new SubtaskDef(seq++, input(q.asText(), model).toString(), AGENT));
            }
            return defs;
        }

        // 阶段②结束:汇总成功的 agent 结论,交给合成
        ArrayNode findings = objectMapper.createArrayNode();
        int maxSeq = 0;
        for (SubtaskEntity a : agents) {
            maxSeq = Math.max(maxSeq, a.getSeq());
            if (!SubtaskStatus.COMPLETED.name().equals(a.getStatus())) {
                continue;
            }
            JsonNode out = json(a.getOutput());
            ObjectNode f = findings.addObject()
                    .put("subQuestion", json(a.getInput()).path("question").asText())
                    .put("answer", out.path("answer").asText());
            f.set("sources", out.path("sources"));
        }
        if (findings.isEmpty()) {
            return List.of();
        }
        ObjectNode in = input(payload.path("question").asText(), model);
        in.set("findings", findings);
        return List.of(new SubtaskDef(maxSeq + 1, in.toString(), SYNTHESIZE));
    }

    private ObjectNode input(String question, String model) {
        ObjectNode n = objectMapper.createObjectNode().put("question", question);
        if (model != null) {
            n.put("model", model);
        }
        return n;
    }

    private static String model(JsonNode payload) {
        return payload != null && payload.hasNonNull("model") ? payload.get("model").asText() : null;
    }

    private JsonNode json(String s) {
        try {
            return objectMapper.readTree(s == null ? "{}" : s);
        } catch (Exception e) {
            throw new IllegalStateException("JSON 解析失败:" + s, e);
        }
    }
}
