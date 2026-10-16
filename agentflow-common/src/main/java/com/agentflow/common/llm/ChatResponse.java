package com.agentflow.common.llm;

import java.util.List;

/** LLM 调用响应:文本内容 + token 用量 + 实际使用的模型名。 */
public class ChatResponse {
    private String content;
    private TokenUsage usage;
    private String model;
    /** 模型要求调用的工具;为空表示这是最终回答。永不为 null。 */
    private List<ToolCall> toolCalls = List.of();

    public ChatResponse() {}

    public ChatResponse(String content, TokenUsage usage, String model) {
        this.content = content;
        this.usage = usage;
        this.model = model;
    }

    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; }
    public TokenUsage getUsage() { return usage; }
    public void setUsage(TokenUsage usage) { this.usage = usage; }
    public String getModel() { return model; }
    public void setModel(String model) { this.model = model; }
    public List<ToolCall> getToolCalls() { return toolCalls; }
    public void setToolCalls(List<ToolCall> toolCalls) { this.toolCalls = toolCalls == null ? List.of() : toolCalls; }
}
