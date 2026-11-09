package com.agentflow.server.product;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/** 推送地址校验(防 SSRF)与各平台的请求体。 */
class WebhookClientTest {

    private static final String SLACK = "https://hooks.slack.com/services/T000/B000/abcdWXYZ";
    private static final String FEISHU = "https://open.feishu.cn/open-apis/bot/v2/hook/0000-1111";
    private static final String LARK = "https://open.larksuite.com/open-apis/bot/v2/hook/0000-2222";

    /** 不发真实请求:记下要发的内容,返回预设的响应体。 */
    static class Recording extends WebhookClient {
        final List<URI> uris = new ArrayList<>();
        final List<String> bodies = new ArrayList<>();
        String response = "ok";

        @Override
        String post(URI uri, String json) {
            uris.add(uri);
            bodies.add(json);
            return response;
        }
    }

    @Test
    void acceptsTheThreeKnownHosts() {
        assertEquals(SLACK, WebhookClient.validate("  " + SLACK + " "));
        assertEquals(FEISHU, WebhookClient.validate(FEISHU));
        assertEquals(LARK, WebhookClient.validate(LARK));
    }

    @Test
    void rejectsEverythingButTheThreeKnownHosts() {
        String[] bad = {
                null, "", "not a url", "file:///etc/passwd",
                "http://hooks.slack.com/services/T/B/x",                 // 明文
                "https://evil.example/services/T/B/x",
                "https://hooks.slack.com.evil.example/services/T/B/x",   // 后缀伪装
                "https://hooks.slack.com@evil.example/services/T/B/x",   // userinfo 伪装
                "https://user:pw@hooks.slack.com/services/T/B/x",
                "https://hooks.slack.com:8443/services/T/B/x",           // 非默认端口
                "https://hooks.slack.com/other/T/B/x",                   // 不是 webhook 路径
                "https://hooks.slack.com\\@evil.example/services/x",
                "https://open.feishu.cn/open-apis/authen/v1/access_token",
                "https://open.feishu.cn.evil.example/open-apis/bot/v2/hook/x",
                "https://127.0.0.1/services/T/B/x",
                "https://localhost/open-apis/bot/v2/hook/x",
                "https://[::1]/services/T/B/x",
                "https://169.254.169.254/latest/meta-data/",
                "https://hooks.slack.com/services/" + "a".repeat(600),   // 超过列宽
        };
        for (String url : bad) {
            assertThrows(IllegalArgumentException.class, () -> WebhookClient.validate(url), String.valueOf(url));
        }
        Recording client = new Recording();
        assertThrows(IllegalArgumentException.class, () -> client.send("https://evil.example/x", "hi"));
        assertTrue(client.uris.isEmpty(), "发送前会再校验一次,库里存了坏地址也发不出去");
    }

    @Test
    void postsTheRightBodyPerPlatform() throws Exception {
        ObjectMapper om = new ObjectMapper();
        Recording client = new Recording();

        client.send(SLACK, "AAPL <!channel> & more");
        JsonNode slack = om.readTree(client.bodies.get(0));
        assertEquals("AAPL &lt;!channel&gt; &amp; more", slack.get("text").asText(), "Slack 的 <!channel> 不能生效");
        assertEquals(URI.create(SLACK), client.uris.get(0));

        client.response = "{\"code\":0,\"msg\":\"success\"}";
        client.send(FEISHU, "AAPL <at user_id=\"all\">x</at>");
        JsonNode feishu = om.readTree(client.bodies.get(1));
        assertEquals("text", feishu.get("msg_type").asText());
        assertFalse(feishu.get("content").get("text").asText().contains("<"), "飞书的 <at> 不能生效");
        assertTrue(feishu.get("content").get("text").asText().startsWith("AAPL "));
    }

    @Test
    void feishuErrorCodeInA200ResponseIsAFailure() {
        Recording client = new Recording();
        client.response = "{\"code\":19001,\"msg\":\"param invalid: incoming webhook access token invalid\"}";
        assertThrows(IllegalStateException.class, () -> client.send(FEISHU, "hi"));
    }
}
