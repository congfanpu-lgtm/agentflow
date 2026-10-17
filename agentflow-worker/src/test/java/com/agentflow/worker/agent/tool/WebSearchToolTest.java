package com.agentflow.worker.agent.tool;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** WebSearchTool:有 Tavily key 走 Tavily,没有则退回 DuckDuckGo HTML 版(本地桩,不出网)。 */
class WebSearchToolTest {

    private static final ObjectMapper OM = new ObjectMapper();

    /** 结构取自真实的 html.duckduckgo.com 结果页:跳转链接里 uddg 参数才是真 URL;含一条广告(y.js)。 */
    static final String DDG_HTML = """
            <div class="result results_links result--ad">
              <a rel="nofollow" class="result__a" href="https://duckduckgo.com/y.js?ad_domain=x&amp;u3=1">Ad title</a>
              <a class="result__snippet" href="https://duckduckgo.com/y.js?u3=1">buy now</a>
            </div>
            <div class="result results_links results_links_deep web-result ">
              <h2 class="result__title">
                <a rel="nofollow" class="result__a" href="//duckduckgo.com/l/?uddg=https%3A%2F%2Ffinance.yahoo.com%2Fmarkets%2Fetf%2Ddrop%2D1.html&amp;rut=99d2">Exchange-Traded Funds Drop as US Equities Fall</a>
              </h2>
              <a class="result__snippet" href="//duckduckgo.com/l/?uddg=x">Actively traded Invesco <b>QQQ</b> Trust (<b>QQQ</b>) eased 1.8%. Investors rotated out of tech.</a>
            </div>
            <div class="result results_links results_links_deep web-result ">
              <h2 class="result__title">
                <a rel="nofollow" class="result__a" href="//duckduckgo.com/l/?uddg=https%3A%2F%2Fwww.whyisstock.com%2Fstocks%2FQQQ&amp;rut=8987">Why Is Invesco QQQ Down Today? | WhyIs ...</a>
              </h2>
              <a class="result__snippet" href="//duckduckgo.com/l/?uddg=y">QQQ is down 2.50% <b>today</b>. What&#x27;s driving the move.</a>
            </div>
            """;

    private HttpServer server;
    private final AtomicReference<String> tavilyAuth = new AtomicReference<>();
    private final AtomicReference<String> tavilyBody = new AtomicReference<>();
    private final AtomicReference<String> ddgQuery = new AtomicReference<>();
    private volatile String ddgResponse = DDG_HTML;

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/search", ex -> {
            tavilyAuth.set(ex.getRequestHeaders().getFirst("Authorization"));
            tavilyBody.set(new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8));
            respond(ex, """
                    {"results":[{"title":"QQQ falls as chip stocks slide","url":"https://www.reuters.com/markets/qqq-1",
                                 "content":"The Nasdaq-100 tracking fund fell 2.4% on Thursday.","score":0.9}]}
                    """);
        });
        server.createContext("/html/", ex -> {
            ddgQuery.set(ex.getRequestURI().getRawQuery());
            respond(ex, ddgResponse);
        });
        server.start();
    }

    private static void respond(com.sun.net.httpserver.HttpExchange ex, String body) throws java.io.IOException {
        byte[] b = body.getBytes(StandardCharsets.UTF_8);
        ex.sendResponseHeaders(200, b.length);
        ex.getResponseBody().write(b);
        ex.close();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private String base() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private static ObjectNode args(String query, String timeRange) {
        ObjectNode a = OM.createObjectNode().put("query", query);
        if (timeRange != null) {
            a.put("time_range", timeRange);
        }
        return a;
    }

    @Test
    void usesTavilyWhenKeyConfigured() throws Exception {
        WebSearchTool tool = new WebSearchTool("tvly-test", base(), base());
        String out = tool.run(args("QQQ drop", "day"));

        assertEquals("Bearer tvly-test", tavilyAuth.get());
        JsonNode body = OM.readTree(tavilyBody.get());
        assertEquals("QQQ drop", body.path("query").asText());
        assertEquals("day", body.path("time_range").asText());
        assertEquals(WebSearchTool.MAX_RESULTS, body.path("max_results").asInt());
        assertNull(ddgQuery.get(), "有 key 时不应访问 DuckDuckGo");
        assertTrue(out.contains("Title: QQQ falls as chip stocks slide"), out);
        assertTrue(out.contains("URL: https://www.reuters.com/markets/qqq-1"), out);
        assertTrue(out.contains("Snippet: The Nasdaq-100 tracking fund fell 2.4% on Thursday."), out);
    }

    @Test
    void fallsBackToDuckDuckGoAndDecodesRealUrls() throws Exception {
        WebSearchTool tool = new WebSearchTool("", base(), base());
        String out = tool.run(args("QQQ drop", "day"));

        assertTrue(ddgQuery.get().contains("q=QQQ"), ddgQuery.get());
        assertTrue(ddgQuery.get().contains("df=d"), "time_range=day 应映射为 df=d:" + ddgQuery.get());
        assertTrue(out.contains("URL: https://finance.yahoo.com/markets/etf-drop-1.html"), out);
        assertTrue(out.contains("URL: https://www.whyisstock.com/stocks/QQQ"), out);
        assertTrue(out.contains("Snippet: Actively traded Invesco QQQ Trust (QQQ) eased 1.8%."), out);
        assertTrue(out.contains("What's driving the move."), "HTML 实体应解码:" + out);
        assertFalse(out.contains("<b>"), out);
        assertFalse(out.contains("y.js") || out.contains("Ad title"), "广告结果应被过滤:" + out);
    }

    @Test
    void duckDuckGoWithoutResultsFails() {
        ddgResponse = "<html><body>If this error persists, please let us know: anomaly</body></html>";
        WebSearchTool tool = new WebSearchTool("", base(), base());
        // 抛出 → AgentLoop 转成 "ERROR: ..." 回给模型,模型可改用 wiki_search,任务不中断
        assertThrows(IllegalStateException.class, () -> tool.run(args("QQQ", null)));
    }

    @Test
    void unknownTimeRangeIsIgnoredAndBlankQueryRejected() throws Exception {
        WebSearchTool tool = new WebSearchTool("", base(), base());
        tool.run(args("kafka", "forever"));
        assertFalse(ddgQuery.get().contains("df="), ddgQuery.get());
        assertThrows(IllegalArgumentException.class, () -> tool.run(args("  ", null)));
    }
}
