package com.agentflow.server.controller.dto;

public record TraceView(String stage, String status, Long subtaskId, String detail, long timestamp) {}
