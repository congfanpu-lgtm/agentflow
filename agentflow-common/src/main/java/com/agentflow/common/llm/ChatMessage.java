package com.agentflow.common.llm;

import java.util.List;

/**
 * LLM 对话消息:role = system/user/assistant/tool。
 * assistant 可带 toolCalls(模型决定调工具);tool 消息带 toolCallId(回传对应工具的结果)。
 */
public class ChatMessage {
    private String role;
    private String content;
    private List<ToolCall> toolCalls;
    private String toolCallId;

    public ChatMessage() {}

    public ChatMessage(String role, String content) {
        this.role = role;
        this.content = content;
    }

    public static ChatMessage assistant(String content, List<ToolCall> toolCalls) {
        ChatMessage m = new ChatMessage("assistant", content);
        m.toolCalls = toolCalls;
        return m;
    }

    public static ChatMessage tool(String toolCallId, String content) {
        ChatMessage m = new ChatMessage("tool", content);
        m.toolCallId = toolCallId;
        return m;
    }

    public String getRole() { return role; }
    public void setRole(String role) { this.role = role; }
    public String getContent() { return content; }
    public void setContent(String content) { this.content = content; }
    public List<ToolCall> getToolCalls() { return toolCalls; }
    public void setToolCalls(List<ToolCall> toolCalls) { this.toolCalls = toolCalls; }
    public String getToolCallId() { return toolCallId; }
    public void setToolCallId(String toolCallId) { this.toolCallId = toolCallId; }
}
