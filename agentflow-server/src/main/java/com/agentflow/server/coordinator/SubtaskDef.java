package com.agentflow.server.coordinator;

/** 拆解产物:一个待创建子任务的定义。W3+ 做 DAG 时在此加 dependsOnSeq 字段。 */
public record SubtaskDef(int seq, String inputJson) {}
