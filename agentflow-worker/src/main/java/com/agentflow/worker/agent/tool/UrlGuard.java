package com.agentflow.worker.agent.tool;

import org.springframework.stereotype.Component;

import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.URI;
import java.net.UnknownHostException;

/**
 * SSRF 防护:web_fetch 的 URL 由模型给出(甚至可能来自网页里的注入指令),不能让它访问
 * docker 内网的 MySQL/Redis、云厂商元数据地址或 tailnet。只放行 http/https + 公网地址。
 * ponytail: 校验与真正连接之间各解析一次 DNS(存在 DNS rebinding 窗口);需要时改为解析一次后按 IP 直连。
 */
@Component
public class UrlGuard {

    public void check(URI uri) {
        String scheme = uri.getScheme();
        if (!"http".equalsIgnoreCase(scheme) && !"https".equalsIgnoreCase(scheme)) {
            throw new IllegalArgumentException("只允许 http/https:" + uri);
        }
        String host = uri.getHost();
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException("URL 缺少主机名:" + uri);
        }
        InetAddress[] addrs;
        try {
            addrs = InetAddress.getAllByName(host);
        } catch (UnknownHostException e) {
            throw new IllegalArgumentException("无法解析主机:" + host);
        }
        for (InetAddress a : addrs) {
            if (isBlocked(a)) {
                throw new IllegalArgumentException("拒绝访问内网地址:" + host + " → " + a.getHostAddress());
            }
        }
    }

    static boolean isBlocked(InetAddress a) {
        byte[] b = a.getAddress();
        boolean cgnat = b.length == 4 && (b[0] & 0xff) == 100 && (b[1] & 0xc0) == 64;   // 100.64/10(Tailscale 等)
        boolean ula = a instanceof Inet6Address && (b[0] & 0xfe) == 0xfc;                // fc00::/7
        return a.isLoopbackAddress() || a.isSiteLocalAddress() || a.isLinkLocalAddress()
                || a.isAnyLocalAddress() || a.isMulticastAddress() || cgnat || ula;
    }
}
