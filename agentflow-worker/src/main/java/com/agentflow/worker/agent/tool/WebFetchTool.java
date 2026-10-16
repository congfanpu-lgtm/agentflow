package com.agentflow.worker.agent.tool;

import com.agentflow.worker.agent.TextUtil;
import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Component;

import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;

/**
 * 抓取网页正文。手动跟随重定向,每一跳都过 {@link UrlGuard}(防"公网地址 302 到内网")。
 * 只读前 1MB、10s 超时、正文截断到 2000 字。
 * ponytail: 按 UTF-8 解码,不识别其他 charset;遇到乱码页再按 Content-Type 解码。
 */
@Component
public class WebFetchTool implements AgentTool {

    static final int MAX_BYTES = 1_000_000;
    static final int MAX_REDIRECTS = 3;

    private final UrlGuard guard;
    private final HttpClient http = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NEVER)
            .connectTimeout(Duration.ofSeconds(5)).build();

    public WebFetchTool(UrlGuard guard) {
        this.guard = guard;
    }

    @Override
    public String name() {
        return "web_fetch";
    }

    @Override
    public String description() {
        return "抓取一个公开网页,返回正文前 2000 字。url 必须是 http/https 公网地址。";
    }

    @Override
    public String parametersSchema() {
        return "{\"type\":\"object\",\"properties\":{\"url\":{\"type\":\"string\",\"description\":\"要抓取的网页 URL\"}},"
                + "\"required\":[\"url\"]}";
    }

    @Override
    public String run(JsonNode args) throws Exception {
        URI uri = URI.create(args.path("url").asText("").trim());
        for (int hop = 0; hop <= MAX_REDIRECTS; hop++) {
            guard.check(uri);
            HttpRequest req = HttpRequest.newBuilder(uri).GET()
                    .header("User-Agent", WikiSearchTool.USER_AGENT)
                    .timeout(Duration.ofSeconds(10)).build();
            HttpResponse<InputStream> resp = http.send(req, HttpResponse.BodyHandlers.ofInputStream());
            int sc = resp.statusCode();
            if (sc / 100 == 3) {
                resp.body().close();
                String loc = resp.headers().firstValue("location")
                        .orElseThrow(() -> new IllegalStateException("重定向缺少 Location"));
                uri = uri.resolve(loc);
                continue;
            }
            byte[] bytes;
            try (InputStream in = resp.body()) {
                bytes = in.readNBytes(MAX_BYTES);
            }
            if (sc / 100 != 2) {
                throw new IllegalStateException("HTTP " + sc);
            }
            String text = htmlToText(new String(bytes, StandardCharsets.UTF_8));
            return "URL: " + uri + "\n" + TextUtil.truncate(text, TextUtil.MAX_TOOL_CHARS);
        }
        throw new IllegalStateException("重定向超过 " + MAX_REDIRECTS + " 次");
    }

    static String htmlToText(String html) {
        return html.replaceAll("(?is)<(script|style|noscript)[^>]*>.*?</\\1>", " ")
                .replaceAll("(?s)<[^>]+>", " ")
                .replace("&nbsp;", " ").replace("&lt;", "<").replace("&gt;", ">")
                .replace("&quot;", "\"").replace("&#39;", "'").replace("&amp;", "&")
                .replaceAll("\\s+", " ").trim();
    }
}
