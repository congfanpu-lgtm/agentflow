package com.agentflow.worker.llm;

import com.agentflow.common.llm.ChatMessage;
import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import com.agentflow.common.llm.TokenUsage;
import com.agentflow.common.llm.ToolCall;
import com.agentflow.common.llm.ToolSpec;
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
import java.util.ArrayList;
import java.util.List;

/**
 * 真实模型后端(OpenAI 兼容协议:qwen-turbo / gpt-4o-mini / DeepSeek 等)。
 * 仅当 {@code llm.provider=openai} 时激活;默认走 {@link MockLlmClient},CI 不触发、不烧钱。
 * base-url / api-key 由环境变量注入(不落配置文件)。用 JDK HttpClient,不引 SDK。
 * 支持 OpenAI 原生 tool calling(tools / tool_calls / role=tool),DeepSeek、通义千问等兼容服务同样适用。
 */
@Slf4j
@Component
@ConditionalOnProperty(name = "llm.provider", havingValue = "openai")
public class OpenAiLlmClient implements LlmClient {

    private static final ObjectMapper OM = new ObjectMapper();
    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(10)).build();

    @Value("${llm.openai.base-url:${OPENAI_BASE_URL:https://api.openai.com/v1}}")
    private String baseUrl;
    @Value("${llm.openai.api-key:${OPENAI_API_KEY:}}")
    private String apiKey;
    /** 合并进请求体的厂商专有参数(JSON 对象),如 Anthropic 的 {"thinking":{"type":"disabled"}}。 */
    @Value("${llm.openai.extra-body:${OPENAI_EXTRA_BODY:}}")
    private String extraBody;

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
                ObjectNode o = msgs.addObject().put("role", m.getRole());
                if (m.getContent() == null) {
                    o.putNull("content");
                } else {
                    o.put("content", m.getContent());
                }
                if (m.getToolCallId() != null) {
                    o.put("tool_call_id", m.getToolCallId());
                }
                if (m.getToolCalls() != null && !m.getToolCalls().isEmpty()) {
                    ArrayNode calls = o.putArray("tool_calls");
                    for (ToolCall c : m.getToolCalls()) {
                        ObjectNode call = calls.addObject().put("id", c.getId()).put("type", "function");
                        call.putObject("function").put("name", c.getName()).put("arguments", c.getArgumentsJson());
                    }
                }
            }
            if (request.getTools() != null && !request.getTools().isEmpty()) {
                ArrayNode tools = body.putArray("tools");
                for (ToolSpec t : request.getTools()) {
                    ObjectNode fn = tools.addObject().put("type", "function").putObject("function");
                    fn.put("name", t.name()).put("description", t.description());
                    fn.set("parameters", OM.readTree(t.parametersSchema()));
                }
            }
            if (extraBody != null && !extraBody.isBlank()) {
                body.setAll((ObjectNode) OM.readTree(extraBody));
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
            JsonNode msg = root.path("choices").path(0).path("message");
            String content = msg.path("content").isTextual() ? msg.get("content").asText() : null;
            List<ToolCall> calls = new ArrayList<>();
            for (JsonNode c : msg.path("tool_calls")) {
                calls.add(new ToolCall(c.path("id").asText(), c.path("function").path("name").asText(),
                        c.path("function").path("arguments").asText("{}")));
            }
            JsonNode u = root.path("usage");
            TokenUsage usage = new TokenUsage(
                    u.path("prompt_tokens").asInt(0), u.path("completion_tokens").asInt(0));
            ChatResponse out = new ChatResponse(content, usage, request.getModel());
            out.setToolCalls(calls);
            return out;
        } catch (Exception e) {
            throw new RuntimeException("LLM 调用失败 model=" + request.getModel(), e);
        }
    }
}
