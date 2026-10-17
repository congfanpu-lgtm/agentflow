package com.agentflow.worker.agent.tool;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 网页搜索:配置了 TAVILY_API_KEY 走 Tavily(结果质量高、摘要已清洗);否则退回 DuckDuckGo HTML 版(免 key)。
 * 只返回标题 / URL / 摘要,要看全文由模型再调 web_fetch(那里有 SSRF 校验)。
 */
@Component
public class WebSearchTool implements AgentTool {

    static final int MAX_RESULTS = 5;
    private static final ObjectMapper OM = new ObjectMapper();
    /** time_range → DuckDuckGo 的 df 参数;不在表里的值忽略(不限时间)。 */
    private static final Map<String, String> DDG_RANGE = Map.of("day", "d", "week", "w", "month", "m", "year", "y");
    // 一条结果 = result__a(标题 + 跳转链接)…… result__snippet(摘要);广告的链接是 y.js,不含 uddg,会被跳过
    private static final Pattern DDG_RESULT = Pattern.compile(
            "class=\"result__a\" href=\"([^\"]*)\"[^>]*>(.*?)</a>.*?class=\"result__snippet\"[^>]*>(.*?)</a>",
            Pattern.DOTALL);
    private static final Pattern UDDG = Pattern.compile("[?&]uddg=([^&]+)");

    private final String tavilyKey;
    private final String tavilyBase;
    private final String ddgBase;
    private final HttpClient http = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NORMAL)
            .connectTimeout(Duration.ofSeconds(5)).build();

    public WebSearchTool(@Value("${agent.search.tavily-key:${TAVILY_API_KEY:}}") String tavilyKey,
                         @Value("${agent.search.tavily-base-url:https://api.tavily.com}") String tavilyBase,
                         @Value("${agent.search.ddg-base-url:https://html.duckduckgo.com}") String ddgBase) {
        this.tavilyKey = tavilyKey == null ? "" : tavilyKey.trim();
        this.tavilyBase = tavilyBase;
        this.ddgBase = ddgBase;
    }

    @Override
    public String name() {
        return "web_search";
    }

    @Override
    public String description() {
        return "搜索互联网,返回最相关的 " + MAX_RESULTS + " 条结果(标题、URL、摘要)。适合新闻、时事、行情、"
                + "近期发布等需要最新信息的问题;要看某条结果的全文再调 web_fetch。";
    }

    @Override
    public String parametersSchema() {
        return "{\"type\":\"object\",\"properties\":{"
                + "\"query\":{\"type\":\"string\",\"description\":\"搜索关键词,英文效果通常更好\"},"
                + "\"time_range\":{\"type\":\"string\",\"enum\":[\"day\",\"week\",\"month\",\"year\"],"
                + "\"description\":\"可选:只看最近一天/周/月/年的结果\"}},"
                + "\"required\":[\"query\"]}";
    }

    @Override
    public String run(JsonNode args) throws Exception {
        String q = args.path("query").asText("").trim();
        if (q.isEmpty()) {
            throw new IllegalArgumentException("query 不能为空");
        }
        String range = args.path("time_range").asText("");
        if (!DDG_RANGE.containsKey(range)) {
            range = "";
        }
        return tavilyKey.isEmpty() ? duckDuckGo(q, range) : tavily(q, range);
    }

    private String tavily(String q, String range) throws Exception {
        ObjectNode body = OM.createObjectNode().put("query", q).put("max_results", MAX_RESULTS);
        if (!range.isEmpty()) {
            body.put("time_range", range);
        }
        HttpRequest req = HttpRequest.newBuilder(URI.create(tavilyBase + "/search"))
                .header("Content-Type", "application/json")
                .header("Authorization", "Bearer " + tavilyKey)
                .timeout(Duration.ofSeconds(15))
                .POST(HttpRequest.BodyPublishers.ofString(body.toString())).build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() / 100 != 2) {
            throw new IllegalStateException("Tavily HTTP " + resp.statusCode());   // 不回显响应体:可能含 key 信息
        }
        StringBuilder sb = new StringBuilder();
        for (JsonNode r : OM.readTree(resp.body()).path("results")) {
            append(sb, r.path("title").asText(), r.path("url").asText(), r.path("content").asText());
        }
        return result(sb, q);
    }

    private String duckDuckGo(String q, String range) throws Exception {
        String url = ddgBase + "/html/?q=" + URLEncoder.encode(q, StandardCharsets.UTF_8)
                + (range.isEmpty() ? "" : "&df=" + DDG_RANGE.get(range));
        HttpRequest req = HttpRequest.newBuilder(URI.create(url)).GET()
                .header("User-Agent", WikiSearchTool.USER_AGENT)
                .timeout(Duration.ofSeconds(15)).build();
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() / 100 != 2) {
            throw new IllegalStateException("DuckDuckGo HTTP " + resp.statusCode());
        }
        StringBuilder sb = new StringBuilder();
        Matcher m = DDG_RESULT.matcher(resp.body());
        int n = 0;
        while (m.find() && n < MAX_RESULTS) {
            Matcher u = UDDG.matcher(m.group(1).replace("&amp;", "&"));
            if (!u.find()) {
                continue;   // 广告 / 站内链接
            }
            String real = URLDecoder.decode(u.group(1), StandardCharsets.UTF_8);
            if (!real.startsWith("http")) {
                continue;
            }
            append(sb, WebFetchTool.htmlToText(m.group(2)), real, WebFetchTool.htmlToText(m.group(3)));
            n++;
        }
        if (n == 0) {
            // 没解析到结果:多半是 DuckDuckGo 拦截了自动化请求(返回验证页),让模型换工具
            throw new IllegalStateException("DuckDuckGo 没有返回结果(可能被限流),可改用 wiki_search");
        }
        return sb.toString().trim();
    }

    private static void append(StringBuilder sb, String title, String url, String snippet) {
        sb.append("Title: ").append(title).append('\n')
                .append("URL: ").append(url).append('\n')
                .append("Snippet: ").append(snippet).append("\n\n");
    }

    private static String result(StringBuilder sb, String q) {
        return sb.isEmpty() ? "没有找到相关结果:" + q : sb.toString().trim();
    }
}
