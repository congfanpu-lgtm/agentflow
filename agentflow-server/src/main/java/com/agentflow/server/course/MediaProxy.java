package com.agentflow.server.course;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * 页面要的逐字稿、帧列表和帧图片都在媒体服务里,server 原样转给浏览器(媒体服务只监听内网,浏览器够不着)。
 * 只读 GET;路径由调用方拼好并校验。
 */
@Component
public class MediaProxy {

    public record Body(int status, String contentType, byte[] bytes) {
    }

    private final String baseUrl;
    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(3)).build();

    public MediaProxy(@Value("${media.base-url:http://localhost:8010}") String baseUrl) {
        this.baseUrl = baseUrl;
    }

    public Body get(String path) {
        try {
            HttpResponse<byte[]> resp = http.send(HttpRequest.newBuilder()
                    .uri(URI.create(baseUrl + path)).timeout(Duration.ofSeconds(30)).GET().build(),
                    HttpResponse.BodyHandlers.ofByteArray());
            return new Body(resp.statusCode(),
                    resp.headers().firstValue("Content-Type").orElse("application/octet-stream"), resp.body());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "媒体服务不可用");
        } catch (IOException e) {
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, "媒体服务不可用");
        }
    }
}
