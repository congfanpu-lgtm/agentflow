package com.agentflow.worker.media;

import com.agentflow.worker.retry.NonRetryableException;
import com.fasterxml.jackson.databind.JsonNode;
import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** MediaClient 单测:JDK HttpServer 起桩,记录收到的请求(离线可跑)。 */
class MediaClientTest {

    private static final String KEY = "0123456789abcdef0123456789abcdef";
    private HttpServer server;
    private final List<String> seen = new ArrayList<>();   // "METHOD path body"
    private int status = 200;
    private String reply = "{}";

    @BeforeEach
    void up() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext("/", ex -> {
            String body = new String(ex.getRequestBody().readAllBytes(), StandardCharsets.UTF_8);
            seen.add(ex.getRequestMethod() + " " + ex.getRequestURI().getPath() + " " + body);
            byte[] b = reply.getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(status, b.length);
            ex.getResponseBody().write(b);
            ex.close();
        });
        server.start();
    }

    @AfterEach
    void down() {
        server.stop(0);
    }

    private MediaClient client() {
        MediaProperties props = new MediaProperties();
        props.setBaseUrl("http://localhost:" + server.getAddress().getPort());
        return new MediaClient(props);
    }

    @Test
    void importSendsOnlyTheSourceThatWasGiven() {
        reply = "{\"mediaKey\":\"" + KEY + "\",\"title\":\"L1\",\"durationMs\":7101000,\"video\":\"video.mp4\",\"cached\":false}";
        JsonNode meta = client().importMedia("https://youtu.be/abc", null);
        assertEquals(KEY, meta.get("mediaKey").asText());
        assertEquals("POST /media/import {\"url\":\"https://youtu.be/abc\"}", seen.get(0));
        client().importMedia(null, "lec1.mp4");
        assertEquals("POST /media/import {\"path\":\"lec1.mp4\"}", seen.get(1));
    }

    @Test
    void stepsAndReadsUseTheMediaKeyInThePath() {
        reply = "{\"utterances\":[{\"startMs\":0,\"endMs\":4200,\"text\":\"Hi.\"}],\"frames\":[]}";
        MediaClient c = client();
        c.transcribe(KEY);
        c.frames(KEY);
        assertEquals("Hi.", c.transcript(KEY).get("utterances").get(0).get("text").asText());
        assertTrue(c.frameList(KEY).get("frames").isEmpty());
        assertEquals(List.of("POST /media/" + KEY + "/transcribe ", "POST /media/" + KEY + "/frames ",
                "GET /media/" + KEY + "/transcript ", "GET /media/" + KEY + "/frames "), seen);
    }

    @Test
    void aBadRequestIsNotWorthRetrying() {
        status = 400;
        reply = "{\"detail\":\"only YouTube links are accepted, got: 'https://evil.example/x'\"}";
        NonRetryableException e = assertThrows(NonRetryableException.class,
                () -> client().importMedia("https://evil.example/x", null));
        assertTrue(e.getMessage().startsWith("media 400: only YouTube links are accepted"), e.getMessage());
    }

    @Test
    void aServerSideFailureIsRetried() {
        status = 502;
        reply = "{\"detail\":\"download failed: HTTP Error 403\"}";
        RuntimeException e = assertThrows(RuntimeException.class, () -> client().importMedia("https://youtu.be/abc", null));
        assertFalse(e instanceof NonRetryableException);
        assertTrue(e.getMessage().contains("502"), e.getMessage());
    }
}
