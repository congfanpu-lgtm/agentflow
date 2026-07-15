package com.agentflow.worker.rag;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * RAG 检索工具的唯一入口(worker):HTTP 调 Python RAG 服务 `/rag/search`。
 * 与 LLM 经 {@code LlmGateway} 同构——检索也经这一个 client,不给旁路。检索只读、不产生副作用。
 * 用 JDK HttpClient,不引 SDK。
 */
@Component
@RequiredArgsConstructor
public class RagClient {

    private static final ObjectMapper OM = new ObjectMapper();

    private final RagProperties props;
    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(3)).build();

    public List<Hit> search(String query, int topK) {
        try {
            ObjectNode body = OM.createObjectNode();
            body.put("query", query);
            body.put("topK", topK);
            HttpRequest req = HttpRequest.newBuilder()
                    .uri(URI.create(props.getBaseUrl() + "/rag/search"))
                    .header("Content-Type", "application/json")
                    .timeout(Duration.ofMillis(props.getTimeoutMs()))
                    .POST(HttpRequest.BodyPublishers.ofString(OM.writeValueAsString(body)))
                    .build();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() / 100 != 2) {
                throw new IllegalStateException("RAG HTTP " + resp.statusCode() + ": " + resp.body());
            }
            List<Hit> hits = new ArrayList<>();
            for (JsonNode h : OM.readTree(resp.body()).path("hits")) {
                hits.add(new Hit(h.path("id").asText(), h.path("text").asText(),
                        h.path("score").asDouble()));
            }
            return hits;
        } catch (Exception e) {
            throw new RuntimeException("RAG 检索失败 query=" + query, e);
        }
    }
}
