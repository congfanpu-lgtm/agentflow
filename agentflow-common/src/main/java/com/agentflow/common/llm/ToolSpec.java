package com.agentflow.common.llm;

/** 告诉模型"有哪些工具可用":名字、用途、参数 JSON Schema(字符串)。 */
public record ToolSpec(String name, String description, String parametersSchema) {}
