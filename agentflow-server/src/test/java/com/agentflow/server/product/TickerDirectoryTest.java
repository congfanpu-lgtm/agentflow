package com.agentflow.server.product;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** 股票代码目录:用本地桩代替 SEC 的 company_tickers.json。 */
class TickerDirectoryTest {

    private static final String BODY = "{\"0\":{\"cik_str\":320193,\"ticker\":\"AAPL\",\"title\":\"Apple Inc.\"},"
            + "\"1\":{\"cik_str\":1067983,\"ticker\":\"BRK-B\",\"title\":\"BERKSHIRE HATHAWAY INC\"}}";

    private HttpServer server;
    private String url;
    private int status = 200;
    private final AtomicInteger requests = new AtomicInteger();
    private final AtomicReference<String> userAgent = new AtomicReference<>();

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/tickers.json", ex -> {
            requests.incrementAndGet();
            userAgent.set(ex.getRequestHeaders().getFirst("User-Agent"));
            byte[] b = BODY.getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(status, b.length);
            ex.getResponseBody().write(b);
            ex.close();
        });
        server.start();
        url = "http://127.0.0.1:" + server.getAddress().getPort() + "/tickers.json";
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    @Test
    void findsCompanyWithPaddedCikAndLoadsOnlyOnce() {
        TickerDirectory dir = new TickerDirectory(url, "agentflow-test admin@example.com");
        TickerDirectory.Company c = dir.find("AAPL").orElseThrow();
        assertEquals("0000320193", c.cik());
        assertEquals("Apple Inc.", c.name());
        assertEquals("0001067983", dir.find("BRK-B").orElseThrow().cik());
        assertEquals(Optional.empty(), dir.find("ZZZZ"), "目录加载成功但查不到 = 这只股票不存在");
        assertEquals(1, requests.get(), "24 小时内只拉一次");
        assertEquals("agentflow-test admin@example.com", userAgent.get());
    }

    @Test
    void withoutUserAgentNothingIsFetchedAndTickersAreUnverified() {
        TickerDirectory dir = new TickerDirectory(url, " ");
        assertEquals(new TickerDirectory.Company("ZZZZ", null, "ZZZZ"), dir.find("ZZZZ").orElseThrow());
        assertEquals(0, requests.get());
    }

    @Test
    void whenTheDirectoryCannotBeLoadedTickersAreUnverifiedAndRetriesBackOff() {
        status = 403;
        TickerDirectory dir = new TickerDirectory(url, "agentflow-test admin@example.com");
        assertEquals(new TickerDirectory.Company("AAPL", null, "AAPL"), dir.find("AAPL").orElseThrow());
        dir.find("MSFT");
        assertEquals(1, requests.get(), "失败后一段时间内不再重试,不拖慢每次添加");
    }
}
