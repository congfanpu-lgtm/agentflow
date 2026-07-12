package com.agentflow.server.coordinator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/** ECHO_BATCH:把 items 数组逐项拆成并行子任务(fan-out)。 */
@Component
@RequiredArgsConstructor
public class EchoBatchDecomposer implements TaskDecomposer {

    private final ObjectMapper objectMapper;

    @Override
    public String type() {
        return "ECHO_BATCH";
    }

    @Override
    public List<SubtaskDef> decompose(JsonNode payload) {
        JsonNode items = payload == null ? null : payload.get("items");
        if (items == null || !items.isArray() || items.isEmpty()) {
            throw new IllegalArgumentException("payload.items 必须是非空数组");
        }
        List<SubtaskDef> defs = new ArrayList<>();
        for (int i = 0; i < items.size(); i++) {
            var input = objectMapper.createObjectNode().put("text", items.get(i).asText());
            defs.add(new SubtaskDef(i, input.toString()));
        }
        return defs;
    }
}
