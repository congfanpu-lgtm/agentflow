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
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * RESEARCH_AGENT:plan → map →(可选:reflect → 追问 map,最多 N 轮)→ reduce。
 * 提交时只建 1 个 PLAN。PLAN 先分流(见 worker 的 PlanProcessor):说明类问题拆成 N 个子问题并行查;
 * 答案类问题(要一个具体事实,往往后一步要用前一步的答案)不拆,原问题整个交给 1 个 AGENT 顺着查 ——
 * 拆开并行跑时后面的 agent 不知道前面查到了什么(FRAMES 评测:拆 13/30,不拆 22/30)。
 * AGENT 全部落定且至少 1 个成功 → SYNTHESIZE。
 * <p>反思默认关闭,payload.rounds=1..{@link #MAX_ROUNDS}(或 payload.reflect=true,等于 1 轮)才开:REFLECT 判断现有结论能不能回答原问题,
 * 不能 → 最多 {@link #MAX_FOLLOW_UPS} 个追问 AGENT → 再 REFLECT……直到能回答或用完轮数。REFLECT 失败不拖垮任务,直接合成。
 * 两次评测都不划算:说明类问题开一轮,深度 95%→95%、成本 +85%(eval/results/v2-*);
 * 答案类问题开 4 轮,没见过的 30 题 17→19(误差内)、token ×2(eval/results/frames.md)。
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
    static final int MAX_FOLLOW_UPS = 4;   // 每轮追问数,与拆题上限一致
    static final int MAX_ROUNDS = 4;

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
        SubtaskEntity plan = subs.stream().filter(s -> PLAN.equals(s.getKind())).findFirst().orElse(null);

        if (agents.isEmpty()) {   // 阶段①刚结束:按拆题结果扇出 agent
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
        SubtaskEntity reflect = subs.stream().filter(s -> REFLECT.equals(s.getKind()))
                .max(Comparator.comparingInt(SubtaskEntity::getSeq)).orElse(null);   // 最近一次反思
        long reflects = subs.stream().filter(s -> REFLECT.equals(s.getKind())).count();
        boolean followUpsCreated = reflect != null && agents.stream().anyMatch(a -> a.getSeq() > reflect.getSeq());
        JsonNode kind = plan == null ? null : json(plan.getOutput()).get("kind");
        if ((reflect == null || followUpsCreated) && reflects < rounds(payload)) {
            ObjectNode in = input(payload.path("question").asText(), model);
            in.set("findings", findings);
            return List.of(new SubtaskDef(maxSeq + 1, in.toString(), REFLECT));
        }
        if (reflect != null && !followUpsCreated && SubtaskStatus.COMPLETED.name().equals(reflect.getStatus())) {
            Set<String> asked = new HashSet<>();   // 已经查过的问题不再派,否则同一个追问会空转到轮数用完
            agents.forEach(a -> asked.add(json(a.getInput()).path("question").asText()));
            List<SubtaskDef> defs = new ArrayList<>();
            for (JsonNode gap : json(reflect.getOutput()).path("gaps")) {
                if (defs.size() == MAX_FOLLOW_UPS) {
                    break;
                }
                String q = gap.asText().strip();   // 模型写的追问和用户问题一样封顶
                q = q.length() > MAX_QUESTION_CHARS ? q.substring(0, MAX_QUESTION_CHARS) : q;
                if (!q.isEmpty() && asked.add(q)) {
                    defs.add(new SubtaskDef(maxSeq + 1 + defs.size(),
                            input(q, model).put("followUp", true).toString(), AGENT));
                }
            }
            if (!defs.isEmpty()) {
                return defs;
            }
        }
        ObjectNode in = input(payload.path("question").asText(), model);
        in.set("findings", findings);
        if (kind != null) {
            in.set("kind", kind);   // 答案类的报告要先给答案
        }
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

    /** 反思轮数:payload.rounds(上限 {@link #MAX_ROUNDS});没给时 payload.reflect=true(页面的 Deep review 开关)算 1 轮,否则 0。 */
    static int rounds(JsonNode payload) {
        int r = payload.path("rounds").asInt(payload.path("reflect").asBoolean(false) ? 1 : 0);
        return Math.max(0, Math.min(r, MAX_ROUNDS));
    }

    private JsonNode json(String s) {
        try {
            return objectMapper.readTree(s == null ? "{}" : s);
        } catch (Exception e) {
            throw new IllegalStateException("JSON 解析失败:" + s, e);
        }
    }
}
