package com.agentflow.server.product;

import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseCookie;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ResponseStatusException;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.Base64;
import java.util.HexFormat;

/**
 * 登录会话:随机令牌放在 HttpOnly cookie 里,Redis 里只存令牌的 SHA-256 → 用户 id
 * (Redis 被读到也拿不到能用的令牌)。SameSite=Lax 让别的网站发起的 POST 带不上这个 cookie。
 */
@Component
public class Sessions {

    static final String COOKIE = "AF_SESSION";
    private static final String PREFIX = "agentflow:session:";
    private static final Duration TTL = Duration.ofDays(30);
    private static final SecureRandom RANDOM = new SecureRandom();

    private final RedissonClient redisson;
    private final boolean secureCookie;

    public Sessions(RedissonClient redisson,
                    @Value("${agentflow.auth.cookie-secure:${AUTH_COOKIE_SECURE:false}}") boolean secureCookie) {
        this.redisson = redisson;
        this.secureCookie = secureCookie;
    }

    public void create(long userId, HttpServletResponse resp) {
        byte[] raw = new byte[32];
        RANDOM.nextBytes(raw);
        String token = Base64.getUrlEncoder().withoutPadding().encodeToString(raw);
        bucket(token).set(String.valueOf(userId), TTL);
        setCookie(resp, token, TTL);
    }

    /** 当前登录的用户 id;没登录返回 null。 */
    public Long resolve(HttpServletRequest req) {
        String token = token(req);
        String userId = token == null ? null : bucket(token).get();
        return userId == null ? null : Long.valueOf(userId);
    }

    /** 当前登录的用户 id;没登录抛 401。每个需要登录的接口都从这里取用户 id。 */
    public long require(HttpServletRequest req) {
        Long userId = resolve(req);
        if (userId == null) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "请先登录");
        }
        return userId;
    }

    public void destroy(HttpServletRequest req, HttpServletResponse resp) {
        String token = token(req);
        if (token != null) {
            bucket(token).delete();
        }
        setCookie(resp, "", Duration.ZERO);
    }

    private void setCookie(HttpServletResponse resp, String value, Duration maxAge) {
        resp.addHeader(HttpHeaders.SET_COOKIE, ResponseCookie.from(COOKIE, value)
                .httpOnly(true).secure(secureCookie).sameSite("Lax").path("/").maxAge(maxAge).build().toString());
    }

    private static String token(HttpServletRequest req) {
        if (req.getCookies() != null) {
            for (Cookie c : req.getCookies()) {
                if (COOKIE.equals(c.getName()) && !c.getValue().isBlank() && c.getValue().length() <= 100) {
                    return c.getValue();
                }
            }
        }
        return null;
    }

    private RBucket<String> bucket(String token) {
        try {
            byte[] digest = MessageDigest.getInstance("SHA-256").digest(token.getBytes(StandardCharsets.UTF_8));
            return redisson.getBucket(PREFIX + HexFormat.of().formatHex(digest), StringCodec.INSTANCE);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
