package com.agentflow.server.coordinator;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

/** 协调器拆解策略:每种任务类型一个实现,Spring 自动注册(策略模式)。 */
public interface TaskDecomposer {
    String type();
    List<SubtaskDef> decompose(JsonNode payload);
}
