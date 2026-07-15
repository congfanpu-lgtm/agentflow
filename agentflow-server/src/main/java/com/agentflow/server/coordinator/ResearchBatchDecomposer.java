package com.agentflow.server.coordinator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/** RESEARCH_BATCH:把 questions 数组逐项拆成并行子任务,每项经意图门控 + RAG 检索 + LLM 汇总。 */
@Component
@RequiredArgsConstructor
public class ResearchBatchDecomposer implements TaskDecomposer {

    private final ObjectMapper objectMapper;

    @Override
    public String type() {
        return "RESEARCH_BATCH";
    }

    @Override
    public List<SubtaskDef> decompose(JsonNode payload) {
        JsonNode questions = payload == null ? null : payload.get("questions");
        if (questions == null || !questions.isArray() || questions.isEmpty()) {
            throw new IllegalArgumentException("payload.questions 必须是非空数组");
        }
        String model = payload.hasNonNull("model") ? payload.get("model").asText() : null;
        List<SubtaskDef> defs = new ArrayList<>();
        for (int i = 0; i < questions.size(); i++) {
            ObjectNode input = objectMapper.createObjectNode().put("question", questions.get(i).asText());
            if (model != null) {
                input.put("model", model);
            }
            defs.add(new SubtaskDef(i, input.toString()));
        }
        return defs;
    }
}
