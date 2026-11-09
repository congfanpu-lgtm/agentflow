package com.agentflow.server.product;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;

/**
 * 往用户自己填的 Slack / 飞书群机器人地址发消息。
 * 这个地址是用户输入、由服务器去请求的,所以只认三个固定的主机 + 固定的路径前缀(不是"不许内网地址"的黑名单),
 * 且不跟随跳转 —— 填什么都没法让服务器去访问内网或别的网站。
 */
@Component
public class WebhookClient {

    private static final ObjectMapper OM = new ObjectMapper();
    /** 主机 → 必须的路径前缀。 */
    private static final Map<String, String> ALLOWED = Map.of(
            "hooks.slack.com", "/services/",
            "open.feishu.cn", "/open-apis/bot/v2/hook/",
            "open.larksuite.com", "/open-apis/bot/v2/hook/");

    private final HttpClient http = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NEVER).connectTimeout(Duration.ofSeconds(5)).build();

    /** 返回去掉首尾空格后的地址;不合规抛 IllegalArgumentException(消息可以直接给用户看)。 */
    public static String validate(String url) {
        String u = url == null ? "" : url.trim();
        URI uri;
        try {
            uri = URI.create(u);
        } catch (IllegalArgumentException e) {
            uri = null;
        }
        String prefix = uri == null || uri.getHost() == null ? null : ALLOWED.get(uri.getHost().toLowerCase());
        if (u.length() > 512 || prefix == null || !"https".equalsIgnoreCase(uri.getScheme())
                || uri.getRawUserInfo() != null || uri.getPort() != -1
                || uri.getRawPath() == null || !uri.getRawPath().startsWith(prefix)) {
            throw new IllegalArgumentException("推送地址只支持 Slack(hooks.slack.com/services/…)"
                    + "和飞书群机器人(open.feishu.cn/open-apis/bot/v2/hook/…)的 https 地址");
        }
        return u;
    }

    /** 发一条纯文本消息;失败抛 IllegalStateException。 */
    public void send(String url, String text) {
        URI uri = URI.create(validate(url));   // 发之前再校验一次:不信任库里存的值
        ObjectNode body = OM.createObjectNode();
        if ("hooks.slack.com".equalsIgnoreCase(uri.getHost())) {
            // Slack 用 <…> 表示链接和 @;转义后 <!channel> 之类只会显示成普通文字
            body.put("text", text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"));
        } else {
            // 飞书文本消息里 <at user_id="all"> 会 @所有人;换成全角括号
            body.put("msg_type", "text").putObject("content").put("text", text.replace('<', '＜').replace('>', '＞'));
        }
        String resp = post(uri, body.toString());
        try {
            JsonNode code = OM.readTree(resp).path("code");   // 飞书出错时也返回 200,错误在 code 里
            if (code.isNumber() && code.asInt() != 0) {
                throw new IllegalStateException("推送失败:对方返回错误码 " + code.asInt());
            }
        } catch (IOException notJson) {
            // Slack 成功时返回纯文本 "ok"
        }
    }

    /** 包内可见,测试里替换掉以免真的往外发。返回响应体;非 2xx 抛 IllegalStateException。 */
    String post(URI uri, String json) {
        try {
            HttpResponse<String> resp = http.send(HttpRequest.newBuilder(uri).timeout(Duration.ofSeconds(10))
                    .header("Content-Type", "application/json; charset=utf-8")
                    .POST(HttpRequest.BodyPublishers.ofString(json)).build(), HttpResponse.BodyHandlers.ofString());
            if (resp.statusCode() / 100 != 2) {
                throw new IllegalStateException("推送失败:HTTP " + resp.statusCode());
            }
            return resp.body();
        } catch (IOException e) {
            throw new IllegalStateException("推送失败:" + e.getClass().getSimpleName());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("推送被中断");
        }
    }
}
