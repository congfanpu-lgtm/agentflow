package com.agentflow.server.coordinator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/** LLM_BATCH:把 items 数组逐项拆成并行子任务,每项经网关调 LLM(fan-out)。可选 payload.model 透传。 */
@Component
@RequiredArgsConstructor
public class LlmBatchDecomposer implements TaskDecomposer {

    private final ObjectMapper objectMapper;

    @Override
    public String type() {
        return "LLM_BATCH";
    }

    @Override
    public List<SubtaskDef> decompose(JsonNode payload) {
        JsonNode items = payload == null ? null : payload.get("items");
        if (items == null || !items.isArray() || items.isEmpty()) {
            throw new IllegalArgumentException("payload.items 必须是非空数组");
        }
        String model = payload.hasNonNull("model") ? payload.get("model").asText() : null;
        List<SubtaskDef> defs = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            ObjectNode input = objectMapper.createObjectNode().put("text", items.get(i).asText());
            if (model != null) {
                input.put("model", model);
            }
            defs.add(new SubtaskDef(i, input.toString()));
        }
        return defs;
    }
}
