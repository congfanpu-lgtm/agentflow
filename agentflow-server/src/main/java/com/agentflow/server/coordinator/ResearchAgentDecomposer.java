package com.agentflow.server.coordinator;

import com.agentflow.common.state.SubtaskStatus;
import com.agentflow.common.state.TaskStatus;
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
 * RESEARCH_AGENT:plan → map →(可选:reflect → 追问 map)→ reduce。
 * 提交时只建 1 个 PLAN;PLAN 成功 → N 个 AGENT(并行);AGENT 全部落定且至少 1 个成功 → 1 个 REFLECT
 * 检查结论是否足以回答原问题;有缺口 → 最多 {@link #MAX_FOLLOW_UPS} 个追问 AGENT;之后 → 1 个 SYNTHESIZE。
 * 反思只做一轮(成本可控、不会无限追问);REFLECT 失败不拖垮任务,直接合成。
 * 默认关闭,payload.reflect=true 才开:评测(eval/results/v2-*)里开启后要点深度 95%→95%、
 * 成本 +85%、p50 延迟 21s→39s,在这套题上不划算。
 * 每个阶段的每个子任务都是普通 Kafka 子任务,复用重试 / DLQ / 超时兜底 / 幂等 / trace。
 */
@Component
@RequiredArgsConstructor
public class ResearchAgentDecomposer implements TaskDecomposer {

    static final int MAX_QUESTION_CHARS = 500;
    static final String PLAN = "PLAN";
    static final String AGENT = "AGENT";
    static final String SYNTHESIZE = "SYNTHESIZE";
    static final String REFLECT = "REFLECT";
    static final int MAX_FOLLOW_UPS = 2;

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

        // 阶段②结束:汇总成功的 agent 结论(含追问 agent),交给反思或合成
        int maxSeq = subs.stream().mapToInt(SubtaskEntity::getSeq).max().orElse(0);
        ArrayNode findings = findings(agents);
        if (findings.isEmpty()) {
            return List.of();
        }
        SubtaskEntity reflect = subs.stream().filter(s -> REFLECT.equals(s.getKind())).findFirst().orElse(null);
        if (reflect == null && payload.path("reflect").asBoolean(false)) {
            ObjectNode in = input(payload.path("question").asText(), model);
            in.set("findings", findings);
            return List.of(new SubtaskDef(maxSeq + 1, in.toString(), REFLECT));
        }
        boolean followUpsCreated = reflect != null && agents.stream().anyMatch(a -> a.getSeq() > reflect.getSeq());
        if (reflect != null && !followUpsCreated && SubtaskStatus.COMPLETED.name().equals(reflect.getStatus())) {
            List<SubtaskDef> defs = new ArrayList<>();
            for (JsonNode gap : json(reflect.getOutput()).path("gaps")) {
                if (defs.size() == MAX_FOLLOW_UPS) {
                    break;
                }
                String q = gap.asText().strip();   // 模型写的追问和用户问题一样封顶
                q = q.length() > MAX_QUESTION_CHARS ? q.substring(0, MAX_QUESTION_CHARS) : q;
                defs.add(new SubtaskDef(maxSeq + 1 + defs.size(),
                        input(q, model).put("followUp", true).toString(), AGENT));
            }
            if (!defs.isEmpty()) {
                return defs;
            }
        }
        ObjectNode in = input(payload.path("question").asText(), model);
        in.set("findings", findings);
        return List.of(new SubtaskDef(maxSeq + 1, in.toString(), SYNTHESIZE));
    }

    private ArrayNode findings(List<SubtaskEntity> agents) {
        ArrayNode findings = objectMapper.createArrayNode();
        for (SubtaskEntity a : agents) {
            if (!SubtaskStatus.COMPLETED.name().equals(a.getStatus())) {
                continue;
            }
            JsonNode out = json(a.getOutput());
            ObjectNode f = findings.addObject()
                    .put("subQuestion", json(a.getInput()).path("question").asText())
                    .put("answer", out.path("answer").asText());
            f.set("sources", out.path("sources"));
        }
        return findings;
    }

    /** 研究任务的产物是报告:合成没有成功(拆题失败 / agent 全败 / 合成失败)一律 FAILED,不报"部分失败"。 */
    @Override
    public TaskStatus terminalStatus(TaskEntity task, List<SubtaskEntity> subs, TaskStatus computed) {
        boolean reported = subs.stream().anyMatch(s -> SYNTHESIZE.equals(s.getKind())
                && SubtaskStatus.COMPLETED.name().equals(s.getStatus()));
        return reported ? computed : TaskStatus.FAILED;
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
