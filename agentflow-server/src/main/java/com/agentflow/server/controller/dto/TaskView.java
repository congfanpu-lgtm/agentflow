package com.agentflow.server.controller.dto;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

public record TaskView(
        String taskUuid,
        String type,
        String status,
        Progress progress,
        JsonNode result,
        List<SubtaskView> subtasks
) {
    public record Progress(int total, int done, int failed) {}
    public record SubtaskView(int seq, String status, String error) {}
}
