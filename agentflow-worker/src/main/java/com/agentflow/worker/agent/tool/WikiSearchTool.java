package com.agentflow.worker.agent.tool;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/** 搜索英文 Wikipedia(免费、无需 key):search 接口取前 3 个词条,再逐个取 summary。 */
@Component
public class WikiSearchTool implements AgentTool {

    /** Wikimedia API 政策要求可识别的 User-Agent。 */
    static final String USER_AGENT = "AgentFlow/0.1 (portfolio research-agent demo)";
    private static final ObjectMapper OM = new ObjectMapper();

    private final String baseUrl;
    private final HttpClient http = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .connectTimeout(Duration.ofSeconds(5)).build();

    public WikiSearchTool(@Value("${agent.wiki.base-url:https://en.wikipedia.org}") String baseUrl) {
        this.baseUrl = baseUrl;
    }

    @Override
    public String name() {
        return "wiki_search";
    }

    @Override
    public String description() {
        return "搜索英文 Wikipedia,返回最相关的 3 个词条(标题、URL、摘要)。query 请用英文关键词。";
    }

    @Override
    public String parametersSchema() {
        return "{\"type\":\"object\",\"properties\":{\"query\":{\"type\":\"string\",\"description\":\"英文搜索关键词\"}},"
                + "\"required\":[\"query\"]}";
    }

    @Override
    public String run(JsonNode args) throws Exception {
        String q = args.path("query").asText("").trim();
        if (q.isEmpty()) {
            throw new IllegalArgumentException("query 不能为空");
        }
        JsonNode search = get(baseUrl + "/w/api.php?action=query&list=search&format=json&srlimit=3&srsearch=" + enc(q));
        StringBuilder sb = new StringBuilder();
        for (JsonNode hit : search.path("query").path("search")) {
            String title = hit.path("title").asText();
            String slug = enc(title.replace(' ', '_'));
            JsonNode s = get(baseUrl + "/api/rest_v1/page/summary/" + slug);
            sb.append("标题: ").append(title).append('\n')
                    .append("URL: ").append(s.path("content_urls").path("desktop").path("page")
                            .asText("https://en.wikipedia.org/wiki/" + slug)).append('\n')
                    .append("摘要: ").append(s.path("extract").asText("")).append("\n\n");
        }
        return sb.isEmpty() ? "没有找到相关词条:" + q : sb.toString().trim();
    }

    private JsonNode get(String url) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url)).GET()
                .header("User-Agent", USER_AGENT)
                .timeout(Duration.ofSeconds(10)).build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() / 100 != 2) {
            throw new IllegalStateException("Wikipedia HTTP " + resp.statusCode());
        }
        return OM.readTree(resp.body());
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
