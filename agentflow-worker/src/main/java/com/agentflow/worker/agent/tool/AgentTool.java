package com.agentflow.worker.agent.tool;

import com.fasterxml.jackson.databind.JsonNode;

/**
 * agent 可调用的工具。实现类加 {@code @Component} 即被 AgentLoop 自动收录(与 SkillRegistry 同一套
 * Spring 收集 List 的做法,新增工具零配置)。约定:工具只读、无副作用——这是"崩溃后重做某一步无害"的前提。
 */
public interface AgentTool {

    String name();

    /** 给模型看的用途说明。 */
    String description();

    /** 参数 JSON Schema(字符串)。 */
    String parametersSchema();

    /** 返回给模型看的文本。抛出的异常由 AgentLoop 转成 "ERROR: ..." 回给模型,不中断循环。 */
    String run(JsonNode args) throws Exception;
}
