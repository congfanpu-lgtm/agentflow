package com.agentflow.worker.agent.tool;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.*;

/** WikiSearchTool:用本地桩模拟 Wikipedia 的 search + summary 两个接口。 */
class WikiSearchToolTest {

    private HttpServer server;
    private final AtomicReference<String> userAgent = new AtomicReference<>();

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/w/api.php", ex -> {
            userAgent.set(ex.getRequestHeaders().getFirst("User-Agent"));
            respond(ex, "{\"query\":{\"search\":[{\"title\":\"Apache Kafka\"}]}}");
        });
        server.createContext("/api/rest_v1/page/summary/", ex -> respond(ex,
                "{\"extract\":\"Kafka is a distributed event store.\","
                        + "\"content_urls\":{\"desktop\":{\"page\":\"https://en.wikipedia.org/wiki/Apache_Kafka\"}}}"));
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

    @Test
    void formatsTitleUrlAndSummary() throws Exception {
        WikiSearchTool tool = new WikiSearchTool("http://127.0.0.1:" + server.getAddress().getPort());
        String out = tool.run(new ObjectMapper().createObjectNode().put("query", "kafka"));
        assertTrue(out.contains("标题: Apache Kafka"));
        assertTrue(out.contains("URL: https://en.wikipedia.org/wiki/Apache_Kafka"));
        assertTrue(out.contains("Kafka is a distributed event store."));
        assertTrue(userAgent.get().startsWith("AgentFlow/"));
    }

    @Test
    void blankQueryRejected() {
        WikiSearchTool tool = new WikiSearchTool("http://127.0.0.1:1");
        assertThrows(IllegalArgumentException.class,
                () -> tool.run(new ObjectMapper().createObjectNode().put("query", " ")));
    }
}
