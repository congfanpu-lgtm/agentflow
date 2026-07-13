package com.agentflow.common.llm;

import java.util.List;

/** LLM 调用请求。model 可空 → 网关用默认模型。 */
public class ChatRequest {
    private String model;
    private List<ChatMessage> messages;
    private int maxTokens = 512;

    public ChatRequest() {}

    public ChatRequest(String model, List<ChatMessage> messages, int maxTokens) {
        this.model = model;
        this.messages = messages;
        this.maxTokens = maxTokens;
    }

    public String getModel() { return model; }
    public void setModel(String model) { this.model = model; }
    public List<ChatMessage> getMessages() { return messages; }
    public void setMessages(List<ChatMessage> messages) { this.messages = messages; }
    public int getMaxTokens() { return maxTokens; }
    public void setMaxTokens(int maxTokens) { this.maxTokens = maxTokens; }
}
