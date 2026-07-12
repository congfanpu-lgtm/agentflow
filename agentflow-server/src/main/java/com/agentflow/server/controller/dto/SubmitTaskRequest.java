package com.agentflow.server.controller.dto;

import com.fasterxml.jackson.databind.JsonNode;
import lombok.Data;

@Data
public class SubmitTaskRequest {
    private String type;
    private JsonNode payload;
}
