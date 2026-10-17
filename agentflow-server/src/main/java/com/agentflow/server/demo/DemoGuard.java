package com.agentflow.server.demo;

import com.fasterxml.jackson.databind.JsonNode;
import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.redisson.api.RAtomicLong;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * 公开演示保护。配置了 DEMO_ACCESS_CODE 才生效,否则什么都不做(本地开发 / CI 不受影响)。
 * <ul>
 *   <li>所有 /api/** 必须带 X-Access-Code(常数时间比较);页面本身不拦,页面会提示输入访问码。</li>
 *   <li>提交任务:只允许 RESEARCH_AGENT、不允许指定模型;每 IP 每天最多 N 个;
 *       当天总花费(worker 记账写的 agentflow:cost:day:日期)达到预算后拒绝新任务。</li>
 * </ul>
 * 预算是软上限:检查发生在提交时,已在跑的任务会跑完(单个任务约 $0.04,超出量可控)。
 * 客户端 IP 默认取 TCP 对端;经 Cloudflare Tunnel 时对端是本机,需 DEMO_TRUST_PROXY_HEADER=true 改读 CF-Connecting-IP
 * (只有 8080 不直接对外暴露时才能信任这个头,否则客户端可以伪造它绕过限额)。
 */
@Component
public class DemoGuard extends OncePerRequestFilter {

    static final String HEADER = "X-Access-Code";
    static final String DAILY_COST_PREFIX = "agentflow:cost:day:";   // 与 worker TokenAccountant 约定
    static final String IP_PREFIX = "agentflow:demo:ip:";

    private final RedissonClient redisson;
    private final byte[] code;
    private final int perIpDaily;
    private final long budgetMicro;
    private final boolean trustProxyHeader;

    public DemoGuard(RedissonClient redisson,
                     @Value("${demo.access-code:${DEMO_ACCESS_CODE:}}") String code,
                     @Value("${demo.per-ip-daily:${DEMO_PER_IP_DAILY:5}}") int perIpDaily,
                     @Value("${demo.daily-budget-usd:${DEMO_DAILY_BUDGET_USD:1.0}}") double budgetUsd,
                     @Value("${demo.trust-proxy-header:${DEMO_TRUST_PROXY_HEADER:false}}") boolean trustProxyHeader) {
        this.redisson = redisson;
        this.code = code.trim().getBytes(StandardCharsets.UTF_8);
        this.perIpDaily = perIpDaily;
        this.budgetMicro = Math.round(budgetUsd * 1_000_000);
        this.trustProxyHeader = trustProxyHeader;
    }

    public boolean enabled() {
        return code.length > 0;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse resp, FilterChain chain)
            throws ServletException, IOException {
        if (enabled() && req.getRequestURI().startsWith("/api/")) {
            String given = req.getHeader(HEADER);
            if (given == null || !MessageDigest.isEqual(code, given.trim().getBytes(StandardCharsets.UTF_8))) {
                resp.setStatus(HttpStatus.UNAUTHORIZED.value());
                resp.setContentType("application/json");
                resp.getWriter().write("{\"error\":\"This demo needs an access code.\"}");
                return;
            }
        }
        chain.doFilter(req, resp);
    }

    /** 提交任务前调用;不放行时抛 {@link Rejected}。 */
    public void checkSubmit(HttpServletRequest req, String type, JsonNode payload) {
        if (!enabled()) {
            return;
        }
        if (!"RESEARCH_AGENT".equals(type) || (payload != null && payload.has("model"))) {
            throw new Rejected(HttpStatus.FORBIDDEN, "This demo only runs research tasks on the default model.");
        }
        String day = LocalDate.now(ZoneOffset.UTC).toString();
        if (redisson.getAtomicLong(DAILY_COST_PREFIX + day).get() >= budgetMicro) {
            throw new Rejected(HttpStatus.TOO_MANY_REQUESTS, "Today's demo budget is used up — please try again tomorrow.");
        }
        RAtomicLong n = redisson.getAtomicLong(IP_PREFIX + clientIp(req, trustProxyHeader) + ":" + day);
        long count = n.incrementAndGet();
        if (count == 1) {
            n.expire(Duration.ofDays(2));
        }
        if (count > perIpDaily) {
            throw new Rejected(HttpStatus.TOO_MANY_REQUESTS,
                    "Demo limit: " + perIpDaily + (perIpDaily == 1 ? " research task" : " research tasks")
                            + " per day from one address.");
        }
    }

    static String clientIp(HttpServletRequest req, boolean trustProxyHeader) {
        String cf = trustProxyHeader ? req.getHeader("CF-Connecting-IP") : null;
        return cf != null && !cf.isBlank() ? cf.trim() : req.getRemoteAddr();
    }

    public static class Rejected extends RuntimeException {
        private final HttpStatus status;

        Rejected(HttpStatus status, String message) {
            super(message);
            this.status = status;
        }

        public HttpStatus status() {
            return status;
        }
    }
}
