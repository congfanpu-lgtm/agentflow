package com.agentflow.common.llm;

/** LLM 调用响应:文本内容 + token 用量 + 实际使用的模型名。 */
public class ChatResponse {
    private String content;
    private TokenUsage usage;
    private String model;

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
}
