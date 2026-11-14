package com.agentflow.worker.media;

import com.agentflow.worker.retry.NonRetryableException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;

/**
 * 媒体服务的唯一入口(worker):下载、转写、截帧识别都在那边做,这里只发 HTTP 请求。
 * 那边按内容哈希缓存,同一讲同时来两个请求会排队、第二个拿到缓存,所以这些调用可以放心重试。
 * 和 {@code RagClient} 一样用 JDK HttpClient、固定 HTTP/1.1。
 */
@Component
@RequiredArgsConstructor
public class MediaClient {

    private static final ObjectMapper OM = new ObjectMapper();
    private static final int MAX_DETAIL = 300;

    private final MediaProperties props;
    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_1_1)
            .connectTimeout(Duration.ofSeconds(5)).build();

    /** url 和 path 给一个:YouTube 链接,或者媒体服务 inbox 目录里的文件名。 */
    public JsonNode importMedia(String url, String path) {
        ObjectNode body = OM.createObjectNode();
        if (url != null) {
            body.put("url", url);
        }
        if (path != null) {
            body.put("path", path);
        }
        return call("POST", "/media/import", body.toString());
    }

    public JsonNode transcribe(String mediaKey) {
        return call("POST", "/media/" + mediaKey + "/transcribe", null);
    }

    public JsonNode frames(String mediaKey) {
        return call("POST", "/media/" + mediaKey + "/frames", null);
    }

    public JsonNode transcript(String mediaKey) {
        return call("GET", "/media/" + mediaKey + "/transcript", null);
    }

    public JsonNode frameList(String mediaKey) {
        return call("GET", "/media/" + mediaKey + "/frames", null);
    }

    private JsonNode call(String method, String path, String body) {
        HttpRequest.Builder req = HttpRequest.newBuilder()
                .uri(URI.create(props.getBaseUrl() + path))
                .timeout(Duration.ofMillis(props.getTimeoutMs()));
        if ("GET".equals(method)) {
            req.GET();
        } else {
            req.header("Content-Type", "application/json").POST(body == null
                    ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        }
        HttpResponse<String> resp;
        try {
            resp = http.send(req.build(), HttpResponse.BodyHandlers.ofString());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("媒体服务调用被中断 " + method + " " + path, e);
        } catch (IOException e) {
            throw new IllegalStateException("媒体服务调用失败 " + method + " " + path + ": " + e, e);
        }
        int code = resp.statusCode();
        if (code / 100 == 2) {
            try {
                return OM.readTree(resp.body());
            } catch (IOException e) {
                throw new IllegalStateException("媒体服务返回的不是 JSON " + method + " " + path, e);
            }
        }
        String message = "media " + code + ": " + detail(resp.body());
        // 4xx 是"给的东西不对"(链接不合规、媒体不存在),重试也是同样的结果;其余是那边自己的故障
        if (code / 100 == 4) {
            throw new NonRetryableException(message);
        }
        throw new IllegalStateException(message);
    }

    /** 错误体是 {"detail": …};422 时 detail 是个列表,原样转成文本。 */
    private static String detail(String body) {
        String text = body == null ? "" : body;
        try {
            JsonNode d = OM.readTree(text).path("detail");
            if (!d.isMissingNode()) {
                text = d.isTextual() ? d.asText() : d.toString();
            }
        } catch (IOException ignored) {
            // 不是 JSON:就用原文
        }
        return text.length() > MAX_DETAIL ? text.substring(0, MAX_DETAIL) : text;
    }
}
