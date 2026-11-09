package com.agentflow.server.product;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * 美股代码目录:代码 → 公司名和 SEC 的 CIK 编号。数据是 SEC 公开的 company_tickers.json(约 1 万条),
 * 整份拉下来放内存,24 小时刷新一次。
 * SEC 要求请求带上能联系到部署者的 User-Agent;没配(SEC_USER_AGENT)或拉取失败时不拦用户:
 * 返回"未校验"的结果(公司名用代码代替,没有 CIK → 简报只用新闻、不用公告)。
 */
@Slf4j
@Component
public class TickerDirectory {

    private static final ObjectMapper OM = new ObjectMapper();
    private static final long TTL_MS = Duration.ofHours(24).toMillis();
    private static final long RETRY_MS = Duration.ofMinutes(10).toMillis();

    private final String url;
    private final String userAgent;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    private Map<String, Company> cache;
    private long loadedAt;
    private long failedAt;

    public record Company(String ticker, String cik, String name) {
    }

    public TickerDirectory(@Value("${agentflow.sec.tickers-url:https://www.sec.gov/files/company_tickers.json}") String url,
                           @Value("${agentflow.sec.user-agent:${SEC_USER_AGENT:}}") String userAgent) {
        this.url = url;
        this.userAgent = userAgent.trim();
    }

    /** empty = 目录里确实没有这只股票;目录不可用时返回未校验的结果(cik 为 null)。 */
    public Optional<Company> find(String ticker) {
        Map<String, Company> all = all();
        return all == null ? Optional.of(new Company(ticker, null, ticker)) : Optional.ofNullable(all.get(ticker));
    }

    private synchronized Map<String, Company> all() {
        if (userAgent.isEmpty()) {
            return null;
        }
        long now = System.currentTimeMillis();
        boolean fresh = cache != null && now - loadedAt < TTL_MS;
        if (fresh || (failedAt != 0 && now - failedAt < RETRY_MS)) {
            return cache;   // 刷新失败时继续用旧数据
        }
        try {
            cache = load();
            loadedAt = now;
            failedAt = 0;
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            failedAt = now;
            log.warn("ticker directory load failed, tickers stay unverified for now: {}", e.toString());
        }
        return cache;
    }

    private Map<String, Company> load() throws Exception {
        HttpResponse<String> resp = http.send(HttpRequest.newBuilder(URI.create(url))
                .header("User-Agent", userAgent).timeout(Duration.ofSeconds(15)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new IllegalStateException("HTTP " + resp.statusCode());
        }
        Map<String, Company> out = new HashMap<>();
        for (JsonNode n : OM.readTree(resp.body())) {
            String ticker = n.path("ticker").asText("");
            if (!ticker.isEmpty() && n.path("cik_str").canConvertToLong()) {
                out.put(ticker, new Company(ticker, String.format("%010d", n.get("cik_str").asLong()),
                        n.path("title").asText(ticker)));
            }
        }
        if (out.isEmpty()) {
            throw new IllegalStateException("empty ticker directory");
        }
        return out;
    }
}
