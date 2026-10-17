package com.agentflow.worker.agent.tool;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** WikiSearchTool:用本地桩模拟 Wikipedia 的 generator=search 接口(一次请求拿标题 + 摘要 + URL)。 */
class WikiSearchToolTest {

    /** 真实接口按相关度给 index,但 pages 数组本身不保证有序。 */
    static final String PAGES = """
            {"query":{"pages":[
              {"title":"Raft (algorithm)","index":2,"fullurl":"https://en.wikipedia.org/wiki/Raft_(algorithm)","extract":"Raft is a consensus algorithm."},
              {"title":"Apache Kafka","index":1,"fullurl":"https://en.wikipedia.org/wiki/Apache_Kafka","extract":"Kafka is a distributed event store."}]}}
            """;

    private HttpServer server;
    private final AtomicReference<String> userAgent = new AtomicReference<>();
    private final AtomicInteger requests = new AtomicInteger();
    private final AtomicInteger throttleFirst = new AtomicInteger();

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/w/api.php", ex -> {
            requests.incrementAndGet();
            userAgent.set(ex.getRequestHeaders().getFirst("User-Agent"));
            if (throttleFirst.getAndDecrement() > 0) {
                ex.getResponseHeaders().add("Retry-After", "0");
                ex.sendResponseHeaders(429, -1);
                ex.close();
                return;
            }
            respond(ex, PAGES);
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

    private WikiSearchTool tool() {
        return new WikiSearchTool("http://127.0.0.1:" + server.getAddress().getPort());
    }

    @Test
    void oneRequestReturnsTitleUrlAndSummaryInRelevanceOrder() throws Exception {
        String out = tool().run(new ObjectMapper().createObjectNode().put("query", "kafka"));
        assertEquals(1, requests.get(), "一次请求拿全:原来 1 次搜索 + 3 次摘要,并发 agent 一多就被 Wikipedia 429");
        assertTrue(out.indexOf("Title: Apache Kafka") < out.indexOf("Title: Raft (algorithm)"), out);
        assertTrue(out.contains("URL: https://en.wikipedia.org/wiki/Apache_Kafka"), out);
        assertTrue(out.contains("Summary: Kafka is a distributed event store."), out);
        assertTrue(userAgent.get().startsWith("AgentFlow/") && userAgent.get().contains("github.com"),
                "Wikimedia UA 政策要求带联系方式:" + userAgent.get());
    }

    @Test
    void retriesOnceAfter429() throws Exception {
        throttleFirst.set(1);
        String out = tool().run(new ObjectMapper().createObjectNode().put("query", "kafka"));
        assertEquals(2, requests.get());
        assertTrue(out.contains("Title: Apache Kafka"), out);
    }

    @Test
    void persistent429Fails() {
        throttleFirst.set(5);
        assertThrows(IllegalStateException.class,
                () -> tool().run(new ObjectMapper().createObjectNode().put("query", "kafka")));
    }

    @Test
    void blankQueryRejected() {
        WikiSearchTool tool = new WikiSearchTool("http://127.0.0.1:1");
        assertThrows(IllegalArgumentException.class,
                () -> tool.run(new ObjectMapper().createObjectNode().put("query", " ")));
    }
}
