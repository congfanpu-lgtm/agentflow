package com.agentflow.worker.agent;

/** 一次 agent 运行的身份与输入:taskId/subtaskId 用于 trace,subtaskUuid 用作存档 key。 */
public record AgentRun(Long taskId, Long subtaskId, String subtaskUuid, String question, String model) {}
