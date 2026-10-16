package com.agentflow.worker.agent.tool;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class WebFetchToolTest {

    private static final ObjectMapper OM = new ObjectMapper();
    private HttpServer server;
    private String base;

    /** 测试专用:放行本地桩(127.0.0.1),其余地址仍走真实 SSRF 校验。 */
    private final UrlGuard allowLocalStub = new UrlGuard() {
        @Override
        public void check(URI uri) {
            if (!"127.0.0.1".equals(uri.getHost())) {
                super.check(uri);
            }
        }
    };

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/page", ex -> {
            byte[] b = "<html><head><style>p{}</style><script>alert(1)</script></head><body><p>Kafka&amp;logs</p></body></html>"
                    .getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(200, b.length);
            ex.getResponseBody().write(b);
            ex.close();
        });
        server.createContext("/redirect", ex -> {
            ex.getResponseHeaders().add("Location", "http://169.254.169.254/latest/meta-data");
            ex.sendResponseHeaders(302, -1);
            ex.close();
        });
        server.start();
        base = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private String fetch(UrlGuard guard, String url) throws Exception {
        return new WebFetchTool(guard).run(OM.createObjectNode().put("url", url));
    }

    @Test
    void returnsVisibleTextWithUrlHeader() throws Exception {
        String out = fetch(allowLocalStub, base + "/page");
        assertTrue(out.startsWith("URL: " + base + "/page\n"));
        assertTrue(out.contains("Kafka&logs"));
        assertFalse(out.contains("alert"));
        assertFalse(out.contains("<p>"));
    }

    @Test
    void rejectsRedirectIntoPrivateNetwork() {
        Exception e = assertThrows(IllegalArgumentException.class, () -> fetch(allowLocalStub, base + "/redirect"));
        assertTrue(e.getMessage().contains("内网"));
    }

    @Test
    void realGuardRejectsLoopbackPrivateLinkLocalCgnatAndNonHttp() {
        UrlGuard guard = new UrlGuard();
        for (String url : new String[]{
                "http://127.0.0.1/", "http://localhost:3306/", "http://10.0.0.5/", "http://172.20.0.3:6379/",
                "http://192.168.1.1/", "http://169.254.169.254/", "http://100.65.10.56:8080/",
                "http://[::1]/", "http://0.0.0.0/", "file:///etc/passwd", "ftp://example.com/"}) {
            assertThrows(IllegalArgumentException.class, () -> guard.check(URI.create(url)), url);
        }
    }

    @Test
    void htmlToTextStripsTagsScriptsAndEntities() {
        assertEquals("a b & c", WebFetchTool.htmlToText("<div>a</div><script>x()</script><b>b</b> &amp; c"));
    }
}
