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
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.HexFormat;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 公开演示保护。配置了 DEMO_ACCESS_CODE 才生效,否则什么都不做(本地开发 / CI 不受影响)。
 * <ul>
 *   <li>白名单:只有页面本身(GET / 和 /index.html)不需要访问码,其余一切请求都要带 X-Access-Code
 *       (常数时间比较)。按白名单放行而不是按 "/api/" 前缀拦截:Spring 按解码后的路径路由,
 *       /%61pi/... 或 /api;x/... 这类写法能绕过前缀匹配。同一地址连续输错 {@link #MAX_BAD_CODES} 次后 1 小时内拒绝。</li>
 *   <li>提交任务:只允许 RESEARCH_AGENT、不允许指定模型;每个地址每天最多 N 个(IPv6 按 /64 计,
 *       否则一个客户端换个地址就能绕开);输入不合法被拒的提交退还额度。</li>
 *   <li>预算:max(当天已记账花费, 当天已提交任务数 × 每任务预估成本) 达到上限后拒绝新任务。
 *       花费在 LLM 调用返回后才记账,只看已花费挡不住一波并发提交,所以按预估先占。</li>
 *   <li>DLQ 重放在演示模式下禁用(它能把任意失败子任务按原类型、原模型再跑一遍)。</li>
 * </ul>
 * 客户端 IP 默认取 TCP 对端;经 Cloudflare Tunnel 时对端是本机,需 DEMO_TRUST_PROXY_HEADER=true 改读 CF-Connecting-IP
 * (只有 8080 不直接对外暴露时才能信任这个头,否则客户端可以伪造它绕过限额)。
 */
@Component
public class DemoGuard extends OncePerRequestFilter {

    static final String HEADER = "X-Access-Code";
    static final int MAX_BAD_CODES = 20;
    static final String DAILY_COST_PREFIX = "agentflow:cost:day:";   // 与 worker TokenAccountant 约定
    static final String DAILY_TASKS_PREFIX = "agentflow:demo:tasks:";
    static final String IP_PREFIX = "agentflow:demo:ip:";
    static final String BAD_CODE_PREFIX = "agentflow:demo:badcode:";
    private static final Set<String> PUBLIC_PAGES = Set.of("/", "/index.html", "/app.html", "/favicon.ico");
    /**
     * 产品接口(注册登录、登录用户自己的数据)不需要访问码:它们自己校验登录态。
     * 只放行"原样就是这个形状"的 URI —— 段里只有小写字母和连字符,不含 %、;、. ,
     * 所以解码前后是同一个路径,/api/v1/me/../tasks 这类写法匹配不上,仍然要访问码。
     */
    private static final Pattern PRODUCT_API = Pattern.compile("^/api/v1/(auth|me)(/[a-z-]+)*$");

    private final RedissonClient redisson;
    private final byte[] code;
    private final int perIpDaily;
    private final long budgetMicro;
    private final long taskEstimateMicro;
    private final boolean trustProxyHeader;

    public DemoGuard(RedissonClient redisson,
                     @Value("${demo.access-code:${DEMO_ACCESS_CODE:}}") String code,
                     @Value("${demo.per-ip-daily:${DEMO_PER_IP_DAILY:5}}") int perIpDaily,
                     @Value("${demo.daily-budget-usd:${DEMO_DAILY_BUDGET_USD:1.0}}") double budgetUsd,
                     @Value("${demo.task-cost-estimate-usd:${DEMO_TASK_COST_ESTIMATE_USD:0.05}}") double taskEstimateUsd,
                     @Value("${demo.trust-proxy-header:${DEMO_TRUST_PROXY_HEADER:false}}") boolean trustProxyHeader) {
        this.redisson = redisson;
        this.code = code.trim().getBytes(StandardCharsets.UTF_8);
        this.perIpDaily = perIpDaily;
        this.budgetMicro = Math.round(budgetUsd * 1_000_000);
        this.taskEstimateMicro = Math.round(taskEstimateUsd * 1_000_000);
        this.trustProxyHeader = trustProxyHeader;
    }

    public boolean enabled() {
        return code.length > 0;
    }

    @Override
    protected void doFilterInternal(HttpServletRequest req, HttpServletResponse resp, FilterChain chain)
            throws ServletException, IOException {
        if (!enabled() || ("GET".equals(req.getMethod()) && PUBLIC_PAGES.contains(req.getRequestURI()))
                || PRODUCT_API.matcher(req.getRequestURI()).matches()) {
            chain.doFilter(req, resp);
            return;
        }
        RAtomicLong bad = redisson.getAtomicLong(BAD_CODE_PREFIX + quotaKey(clientIp(req, trustProxyHeader)));
        if (bad.get() >= MAX_BAD_CODES) {
            reject(resp, HttpStatus.TOO_MANY_REQUESTS, "Too many wrong access codes — try again in an hour.");
            return;
        }
        String given = req.getHeader(HEADER);
        if (given == null || !MessageDigest.isEqual(code, given.trim().getBytes(StandardCharsets.UTF_8))) {
            if (given != null && !given.isBlank() && bad.incrementAndGet() == 1) {
                bad.expire(Duration.ofHours(1));
            }
            reject(resp, HttpStatus.UNAUTHORIZED, "This demo needs an access code.");
            return;
        }
        chain.doFilter(req, resp);
    }

    private static void reject(HttpServletResponse resp, HttpStatus status, String message) throws IOException {
        resp.setStatus(status.value());
        resp.setContentType("application/json");
        resp.getWriter().write("{\"error\":\"" + message + "\"}");
    }

    /** 提交任务前调用;不放行时抛 {@link Rejected}。放行后若提交本身失败,调 {@link #refund}。 */
    public void checkSubmit(HttpServletRequest req, String type, JsonNode payload) {
        if (!enabled()) {
            return;
        }
        if (!"RESEARCH_AGENT".equals(type) || (payload != null && payload.has("model"))) {
            throw new Rejected(HttpStatus.FORBIDDEN, "This demo only runs research tasks on the default model.");
        }
        String day = today();
        RAtomicLong n = counter(IP_PREFIX + quotaKey(clientIp(req, trustProxyHeader)) + ":" + day);
        if (n.incrementAndGet() > perIpDaily) {
            n.decrementAndGet();
            throw new Rejected(HttpStatus.TOO_MANY_REQUESTS, "Demo limit: " + perIpDaily
                    + (perIpDaily == 1 ? " research task" : " research tasks") + " per day from one address.");
        }
        RAtomicLong tasks = counter(DAILY_TASKS_PREFIX + day);
        long reserved = tasks.incrementAndGet() * taskEstimateMicro;
        long spent = redisson.getAtomicLong(DAILY_COST_PREFIX + day).get();
        if (Math.max(spent, reserved) > budgetMicro) {
            tasks.decrementAndGet();
            n.decrementAndGet();
            throw new Rejected(HttpStatus.TOO_MANY_REQUESTS, "Today's demo budget is used up — please try again tomorrow.");
        }
    }

    /** 通过了 checkSubmit 但提交被判不合法(如问题过长):退还额度。 */
    public void refund(HttpServletRequest req) {
        if (enabled()) {
            String day = today();
            redisson.getAtomicLong(IP_PREFIX + quotaKey(clientIp(req, trustProxyHeader)) + ":" + day).decrementAndGet();
            redisson.getAtomicLong(DAILY_TASKS_PREFIX + day).decrementAndGet();
        }
    }

    /** 演示模式下禁止的运维操作(DLQ 重放等)。 */
    public void forbidInDemo() {
        if (enabled()) {
            throw new Rejected(HttpStatus.FORBIDDEN, "Not available in the public demo.");
        }
    }

    private RAtomicLong counter(String key) {
        RAtomicLong c = redisson.getAtomicLong(key);
        c.expire(Duration.ofDays(2));
        return c;
    }

    private static String today() {
        return LocalDate.now(ZoneOffset.UTC).toString();
    }

    /** 限流用的客户端标识(与演示配额同一口径),供产品接口的注册 / 登录限流复用。 */
    public String clientKey(HttpServletRequest req) {
        return quotaKey(clientIp(req, trustProxyHeader));
    }

    static String clientIp(HttpServletRequest req, boolean trustProxyHeader) {
        String cf = trustProxyHeader ? req.getHeader("CF-Connecting-IP") : null;
        return cf != null && !cf.isBlank() ? cf.trim() : req.getRemoteAddr();
    }

    /** 限额按什么计:IPv4 整个地址;IPv6 取 /64 前缀(一个用户通常拥有整段 /64)。 */
    static String quotaKey(String ip) {
        if (!ip.contains(":")) {
            return ip;
        }
        try {
            byte[] b = InetAddress.getByName(ip).getAddress();   // IP 字面量,不触发 DNS
            return b.length == 16 ? HexFormat.of().formatHex(b, 0, 8) + "::/64" : ip;
        } catch (IOException e) {
            return ip;
        }
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
