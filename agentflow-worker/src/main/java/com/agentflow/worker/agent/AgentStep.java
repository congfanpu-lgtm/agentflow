package com.agentflow.worker.agent;

import com.agentflow.common.llm.ChatMessage;

import java.util.List;

/** 一步完整的 ReAct 存档:模型的决策(含 toolCalls)+ 每个工具的结果 + 本步 token 用量。 */
public record AgentStep(int step, ChatMessage assistant, List<ChatMessage> toolResults, Usage usage) {}
