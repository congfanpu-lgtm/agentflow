package com.agentflow.common.llm;

/** 模型发起的一次工具调用(OpenAI tool_calls 的一项)。argumentsJson 是模型给的原始 JSON 字符串。 */
public class ToolCall {
    private String id;
    private String name;
    private String argumentsJson;

    public ToolCall() {}

    public ToolCall(String id, String name, String argumentsJson) {
        this.id = id;
        this.name = name;
        this.argumentsJson = argumentsJson;
    }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getArgumentsJson() { return argumentsJson; }
    public void setArgumentsJson(String argumentsJson) { this.argumentsJson = argumentsJson; }
}
