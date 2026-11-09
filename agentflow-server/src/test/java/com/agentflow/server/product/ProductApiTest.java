package com.agentflow.server.product;

import jakarta.servlet.http.Cookie;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/** 产品接口:注册 / 登录 / 会话。 */
@SpringBootTest
@AutoConfigureMockMvc
@Transactional
class ProductApiTest {

    private static final String PASSWORD = "test-password-1";

    @Autowired
    private MockMvc mvc;
    @MockBean
    private KafkaTemplate<String, Object> kafkaTemplate;

    /** 每个用例一个客户端地址,互不占用限流额度。 */
    private final String ip = "10.88." + (int) (Math.random() * 250) + "." + (int) (Math.random() * 250);

    private static String newEmail() {
        return "u" + UUID.randomUUID().toString().replace("-", "") + "@example.com";
    }

    private ResultActions send(MockHttpServletRequestBuilder req, Cookie session) throws Exception {
        req.with(r -> { r.setRemoteAddr(ip); return r; });
        return mvc.perform(session == null ? req : req.cookie(session));
    }

    private ResultActions auth(String action, String email, String password) throws Exception {
        return send(post("/api/v1/auth/" + action).contentType(MediaType.APPLICATION_JSON)
                .content("{\"email\":\"" + email + "\",\"password\":\"" + password + "\"}"), null);
    }

    private Cookie register(String email) throws Exception {
        Cookie c = auth("register", email, PASSWORD).andExpect(status().isOk())
                .andReturn().getResponse().getCookie("AF_SESSION");
        assertNotNull(c, "注册成功应直接登录");
        return c;
    }

    @Test
    void registerThenSessionCookieIdentifiesTheUser() throws Exception {
        String email = newEmail();
        Cookie c = register(email.toUpperCase());   // 邮箱不区分大小写
        assertTrue(c.isHttpOnly());
        assertEquals("Lax", c.getAttribute("SameSite"));
        send(get("/api/v1/me"), c).andExpect(status().isOk())
                .andExpect(jsonPath("$.email", is(email)))
                .andExpect(jsonPath("$.pushHour", is(8)))
                .andExpect(jsonPath("$.passwordHash").doesNotExist());
    }

    @Test
    void noSessionMeans401() throws Exception {
        send(get("/api/v1/me"), null).andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.error", is("请先登录")));
        send(get("/api/v1/me"), new Cookie("AF_SESSION", "made-up-token")).andExpect(status().isUnauthorized());
    }

    @Test
    void badRegistrationsAreRejected() throws Exception {
        String email = newEmail();
        register(email);
        auth("register", email, PASSWORD).andExpect(status().isConflict());
        auth("register", "not-an-email", PASSWORD).andExpect(status().isBadRequest());
        auth("register", newEmail(), "short").andExpect(status().isBadRequest());
    }

    @Test
    void loginAndLogout() throws Exception {
        String email = newEmail();
        register(email);
        auth("login", email, "wrong-password").andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.error", is("邮箱或密码错误")));
        auth("login", newEmail(), PASSWORD).andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.error", is("邮箱或密码错误")));
        Cookie c = auth("login", email, PASSWORD).andExpect(status().isOk())
                .andReturn().getResponse().getCookie("AF_SESSION");
        send(get("/api/v1/me"), c).andExpect(status().isOk());
        send(post("/api/v1/auth/logout"), c).andExpect(status().isOk());
        send(get("/api/v1/me"), c).andExpect(status().isUnauthorized());
    }

    @Test
    void repeatedLoginAttemptsForOneEmailAreThrottled() throws Exception {
        String email = newEmail();
        for (int i = 0; i < 10; i++) {
            auth("login", email, "wrong-password").andExpect(status().isUnauthorized());
        }
        auth("login", email, "wrong-password").andExpect(status().isTooManyRequests());
    }

    @Test
    void registrationsFromOneAddressAreLimitedPerDay() throws Exception {
        for (int i = 0; i < 5; i++) {
            register(newEmail());
        }
        auth("register", newEmail(), PASSWORD).andExpect(status().isTooManyRequests());
    }
}
