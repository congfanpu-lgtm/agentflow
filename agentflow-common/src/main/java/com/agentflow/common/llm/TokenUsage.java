package com.agentflow.common.llm;

/** 一次 LLM 调用的 token 用量(真实模型来自 API usage;mock 用字数估算)。 */
public class TokenUsage {
    private int promptTokens;
    private int completionTokens;

    public TokenUsage() {}

    public TokenUsage(int promptTokens, int completionTokens) {
        this.promptTokens = promptTokens;
        this.completionTokens = completionTokens;
    }

    public int getPromptTokens() { return promptTokens; }
    public void setPromptTokens(int promptTokens) { this.promptTokens = promptTokens; }
    public int getCompletionTokens() { return completionTokens; }
    public void setCompletionTokens(int completionTokens) { this.completionTokens = completionTokens; }
    public int total() { return promptTokens + completionTokens; }
}
