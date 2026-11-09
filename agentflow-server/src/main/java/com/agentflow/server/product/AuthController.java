package com.agentflow.server.product;

import com.agentflow.server.demo.DemoGuard;
import com.agentflow.server.entity.UserEntity;
import com.agentflow.server.mapper.UserMapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.time.Duration;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 注册 / 登录 / 登出。
 * 注册每个地址每天 5 次;登录每个地址 15 分钟 20 次、每个邮箱 15 分钟 10 次(挡撞库和暴力破解)。
 * 登录失败不区分"邮箱不存在"和"密码错",且邮箱不存在时也做一次同样耗时的哈希,响应时间看不出差别。
 * ponytail: 没有邮箱验证和找回密码,上线收真实用户前要补(需要发信服务)。
 */
@RestController
@RequestMapping("/api/v1/auth")
@RequiredArgsConstructor
public class AuthController {

    private static final Pattern EMAIL = Pattern.compile("^[^@\\s]+@[^@\\s]+\\.[^@\\s]+$");
    private static final String DUMMY_HASH = PasswordHasher.hash("no-such-user");
    private static final Duration LOGIN_WINDOW = Duration.ofMinutes(15);

    private final UserMapper userMapper;
    private final Sessions sessions;
    private final Throttle throttle;
    private final DemoGuard demoGuard;

    public record Credentials(String email, String password) {
    }

    @PostMapping("/register")
    public Map<String, String> register(@RequestBody Credentials body, HttpServletRequest req, HttpServletResponse resp) {
        String email = normalize(body.email());
        if (email == null) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "邮箱格式不正确");
        }
        String password = body.password();
        if (password == null || password.length() < 8 || password.length() > 128) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "密码长度需为 8 到 128 位");
        }
        if (!throttle.allow("register:" + demoGuard.clientKey(req), 5, Duration.ofDays(1))) {
            throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS, "今天注册次数太多,请明天再试");
        }
        UserEntity user = new UserEntity();
        user.setEmail(email);
        user.setPasswordHash(PasswordHasher.hash(password));
        try {
            userMapper.insert(user);
        } catch (DuplicateKeyException e) {
            throw new ResponseStatusException(HttpStatus.CONFLICT, "这个邮箱已经注册,请直接登录");
        }
        sessions.create(user.getId(), resp);
        return Map.of("email", email);
    }

    @PostMapping("/login")
    public Map<String, String> login(@RequestBody Credentials body, HttpServletRequest req, HttpServletResponse resp) {
        String email = normalize(body.email());
        String password = body.password();
        if (email == null || password == null || password.length() > 128) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "邮箱或密码错误");
        }
        if (!throttle.allow("login:ip:" + demoGuard.clientKey(req), 20, LOGIN_WINDOW)
                || !throttle.allow("login:email:" + email, 10, LOGIN_WINDOW)) {
            throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS, "尝试次数太多,请 15 分钟后再试");
        }
        UserEntity user = userMapper.selectOne(new QueryWrapper<UserEntity>().eq("email", email));
        boolean ok = PasswordHasher.verify(password, user == null ? DUMMY_HASH : user.getPasswordHash());
        if (user == null || !ok) {
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "邮箱或密码错误");
        }
        sessions.create(user.getId(), resp);
        return Map.of("email", email);
    }

    @PostMapping("/logout")
    public Map<String, Boolean> logout(HttpServletRequest req, HttpServletResponse resp) {
        sessions.destroy(req, resp);
        return Map.of("ok", true);
    }

    /** 去空格、转小写;格式不对返回 null。 */
    private static String normalize(String email) {
        if (email == null) {
            return null;
        }
        String e = email.trim().toLowerCase(Locale.ROOT);
        return e.length() <= 254 && EMAIL.matcher(e).matches() ? e : null;
    }
}
