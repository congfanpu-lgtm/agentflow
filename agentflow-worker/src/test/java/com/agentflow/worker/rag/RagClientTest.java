package com.agentflow.worker.rag;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** RagClient 单测:用 JDK HttpServer 起桩返回固定 JSON,验证解析(离线可跑,不依赖真服务)。 */
class RagClientTest {

    private HttpServer server;
    private int port;

    @BeforeEach
    void up() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        port = server.getAddress().getPort();
        server.createContext("/rag/search", ex -> {
            String body = """
                {"hits":[
                  {"id":"kafka-2","text":"kafka rebalance","score":0.91},
                  {"id":"redis-1","text":"redis lua","score":0.42}
                ]}""";
            byte[] b = body.getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, b.length);
            ex.getResponseBody().write(b);
            ex.close();
        });
        server.start();
    }

    @AfterEach
    void down() {
        server.stop(0);
    }

    private RagClient client() {
        RagProperties props = new RagProperties();
        props.setBaseUrl("http://localhost:" + port);
        return new RagClient(props);
    }

    @Test
    void parsesHitsInOrder() {
        List<Hit> hits = client().search("kafka partition", 2);
        assertEquals(2, hits.size());
        assertEquals("kafka-2", hits.get(0).id());
        assertEquals("kafka rebalance", hits.get(0).text());
        assertEquals(0.91, hits.get(0).score(), 1e-9);
    }
}
