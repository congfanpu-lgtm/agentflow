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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * 搜索英文 Wikipedia(免费、无需 key):generator=search 一次请求拿前 3 个词条的标题、导语摘要和 URL。
 * (早先是 1 次搜索 + 3 次 summary,并发 agent 一多就被 Wikipedia 429;评测里 41 次工具报错有 36 次是它。)
 * 429 时按 Retry-After(封顶 3s)等一次再试。
 */
@Component
public class WikiSearchTool implements AgentTool {

    /** Wikimedia API 政策要求可识别的 User-Agent。 */
    static final String USER_AGENT = "AgentFlow/0.1 (https://github.com/congfanpu-lgtm/agentflow; research-agent demo)";
    static final long MAX_RETRY_WAIT_MS = 3000;
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
        JsonNode res = get(baseUrl + "/w/api.php?action=query&format=json&formatversion=2&generator=search"
                + "&gsrlimit=3&prop=extracts%7Cinfo&exintro=1&explaintext=1&exlimit=3&inprop=url&gsrsearch=" + enc(q));
        List<JsonNode> pages = new ArrayList<>();
        res.path("query").path("pages").forEach(pages::add);
        pages.sort(Comparator.comparingInt(p -> p.path("index").asInt(Integer.MAX_VALUE)));   // index = 相关度排名
        StringBuilder sb = new StringBuilder();
        for (JsonNode p : pages) {
            String title = p.path("title").asText();
            sb.append("Title: ").append(title).append('\n')
                    .append("URL: ").append(p.path("fullurl")
                            .asText("https://en.wikipedia.org/wiki/" + enc(title.replace(' ', '_')))).append('\n')
                    .append("Summary: ").append(p.path("extract").asText("")).append("\n\n");
        }
        return sb.isEmpty() ? "没有找到相关词条:" + q : sb.toString().trim();
    }

    private JsonNode get(String url) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url)).GET()
                .header("User-Agent", USER_AGENT)
                .timeout(Duration.ofSeconds(10)).build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() == 429) {
            long waitMs = resp.headers().firstValue("Retry-After").map(v -> {
                try {
                    return Long.parseLong(v.trim()) * 1000;
                } catch (NumberFormatException e) {
                    return 1000L;   // HTTP-date 形式的 Retry-After 不解析,等 1s
                }
            }).orElse(1000L);
            Thread.sleep(Math.min(waitMs, MAX_RETRY_WAIT_MS));
            resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        }
        if (resp.statusCode() / 100 != 2) {
            throw new IllegalStateException("Wikipedia HTTP " + resp.statusCode());
        }
        return OM.readTree(resp.body());
    }

    private static String enc(String s) {
        return URLEncoder.encode(s, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
