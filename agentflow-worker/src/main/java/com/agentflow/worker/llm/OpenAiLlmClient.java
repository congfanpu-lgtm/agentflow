package com.agentflow.worker.llm;

import com.agentflow.common.llm.ChatMessage;
import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import com.agentflow.common.llm.TokenUsage;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * 真实模型后端(OpenAI 兼容协议:qwen-turbo / gpt-4o-mini / DeepSeek 等)。
 * 仅当 {@code llm.provider=openai} 时激活;默认走 {@link MockLlmClient},CI 不触发、不烧钱。
 * base-url / api-key 由环境变量注入(不落配置文件)。用 JDK HttpClient,不引 SDK。
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "llm.provider", havingValue = "openai")
public class OpenAiLlmClient implements LlmClient {

    private static final ObjectMapper OM = new ObjectMapper();
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(10)).build();

    @Value("${llm.openai.base-url:${OPENAI_BASE_URL:https://api.openai.com/v1}}")
    private String baseUrl;
    @Value("${llm.openai.api-key:${OPENAI_API_KEY:}}")
    private String apiKey;

    @Override
    public String provider() {
        return "openai";
    }

    @Override
    public ChatResponse chat(ChatRequest request) {
        try {
            ObjectNode body = OM.createObjectNode();
            body.put("model", request.getModel());
            body.put("max_tokens", request.getMaxTokens());
            ArrayNode msgs = body.putArray("messages");
            for (ChatMessage m : request.getMessages()) {
                msgs.addObject().put("role", m.getRole()).put("content", m.getContent());
            }
            HttpRequest httpReq = HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + "/chat/completions"))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Bearer " + apiKey)
                    .timeout(Duration.ofSeconds(60))
                    .POST(HttpRequest.BodyPublishers.ofString(OM.writeValueAsString(body)))
                    .build();
            HttpResponse<String> resp = http.send(httpReq, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() / 100 != 2) {
                throw new IllegalStateException("LLM HTTP " + resp.statusCode() + ": " + resp.body());
            }
            JsonNode root = OM.readTree(resp.body());
            String content = root.path("choices").path(0).path("message").path("content").asText();
            JsonNode u = root.path("usage");
            TokenUsage usage = new TokenUsage(
                    u.path("prompt_tokens").asInt(0), u.path("completion_tokens").asInt(0));
            return new ChatResponse(content, usage, request.getModel());
        } catch (Exception e) {
            throw new RuntimeException("LLM 调用失败 model=" + request.getModel(), e);
        }
    }
}
