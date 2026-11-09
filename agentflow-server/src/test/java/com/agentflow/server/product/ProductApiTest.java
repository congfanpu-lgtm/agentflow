package com.agentflow.server.product;

import com.agentflow.server.entity.StockEventEntity;
import com.agentflow.server.mapper.StockEventMapper;
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

import java.time.LocalDate;
import java.time.ZoneId;
import java.util.UUID;

import static org.hamcrest.Matchers.*;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/** 产品接口:注册 / 登录 / 会话 / 自选股 / 设置。 */
@SpringBootTest(properties = "agentflow.sec.user-agent=")   // 不配 SEC User-Agent:股票代码不联网校验
@AutoConfigureMockMvc
@Transactional
class ProductApiTest {

    private static final String PASSWORD = "test-password-1";

    @Autowired
    private MockMvc mvc;
    @MockBean
    private KafkaTemplate<String, Object> kafkaTemplate;
    @MockBean
    private WebhookClient webhookClient;   // validate 是静态方法,不受影响;只是不真的往外发
    @MockBean
    private BriefService briefService;     // 加股票时会触发一次简报;这里只验证触发了,不真的跑任务
    @Autowired
    private StockEventMapper stockEventMapper;

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

    // ---------- 自选股 / 事件 / 设置 ----------

    private static final String SLACK = "https://hooks.slack.com/services/T000/B000/abcdWXYZ";

    private ResultActions json(MockHttpServletRequestBuilder req, String body, Cookie session) throws Exception {
        return send(req.contentType(MediaType.APPLICATION_JSON).content(body), session);
    }

    private ResultActions addTicker(String ticker, Cookie session) throws Exception {
        return json(post("/api/v1/me/watchlist"), "{\"ticker\":\"" + ticker + "\"}", session);
    }

    private void insertEvent(String ticker, String title, LocalDate briefDate) {
        StockEventEntity e = new StockEventEntity();
        e.setTicker(ticker);
        e.setBriefDate(briefDate);
        e.setTitle(title);
        e.setSummary("摘要");
        e.setImportance(2);
        e.setUrl("https://example.com/" + UUID.randomUUID());
        e.setUrlHash(UUID.randomUUID().toString().replace("-", "") + UUID.randomUUID().toString().replace("-", ""));
        stockEventMapper.insertIgnore(e);
    }

    @Test
    void usersAreIsolated() throws Exception {
        // 用测试专用的假代码,不和库里已有的真实事件混在一起
        Cookie a = register(newEmail());
        Cookie b = register(newEmail());
        addTicker("zqxa", a).andExpect(status().isOk());
        addTicker("ZQXB", b).andExpect(status().isOk());
        LocalDate today = LocalDate.now(ZoneId.of("America/New_York"));
        insertEvent("ZQXA", "A 的事件", today);
        insertEvent("ZQXB", "B 的事件", today);
        insertEvent("ZQXA", "太早的事件", today.minusDays(40));

        send(get("/api/v1/me/watchlist"), a).andExpect(jsonPath("$", hasSize(1)))
                .andExpect(jsonPath("$[0].ticker", is("ZQXA")));
        send(get("/api/v1/me/events"), a).andExpect(status().isOk()).andExpect(jsonPath("$", hasSize(1)))
                .andExpect(jsonPath("$[0].title", is("A 的事件")));
        send(get("/api/v1/me/events?days=9999"), a).andExpect(jsonPath("$", hasSize(1)));   // 最多看 30 天
        send(get("/api/v1/me/events"), b).andExpect(jsonPath("$", hasSize(1)))
                .andExpect(jsonPath("$[0].title", is("B 的事件")));

        // B 删 A 的股票:删不到;A 的还在
        send(delete("/api/v1/me/watchlist?ticker=ZQXA"), b).andExpect(status().isOk());
        send(get("/api/v1/me/watchlist"), a).andExpect(jsonPath("$", hasSize(1)));
        // A 自己删:事件也跟着看不到了
        send(delete("/api/v1/me/watchlist?ticker=zqxa"), a).andExpect(status().isOk());
        send(get("/api/v1/me/watchlist"), a).andExpect(jsonPath("$", hasSize(0)));
        send(get("/api/v1/me/events"), a).andExpect(jsonPath("$", hasSize(0)));

        // 没登录:什么都拿不到
        send(get("/api/v1/me/events"), null).andExpect(status().isUnauthorized());
        addTicker("ZQXA", null).andExpect(status().isUnauthorized());
        json(put("/api/v1/me/settings"), "{\"pushHour\":9}", null).andExpect(status().isUnauthorized());
        send(post("/api/v1/me/webhook-test"), null).andExpect(status().isUnauthorized());
    }

    @Test
    void watchlistAcceptsValidTickersOnceAndStopsAtTwenty() throws Exception {
        Cookie c = register(newEmail());
        for (String bad : new String[]{"", "TOOLONG", "A1", "AAPL;DROP", "<b>", "BRK.B.C"}) {
            addTicker(bad, c).andExpect(status().isBadRequest());
        }
        json(post("/api/v1/me/watchlist"), "{}", c).andExpect(status().isBadRequest());
        addTicker("brk.b", c).andExpect(status().isOk()).andExpect(jsonPath("$.ticker", is("BRK-B")));
        addTicker("BRK-B", c).andExpect(status().isOk());   // 重复添加不报错
        send(get("/api/v1/me/watchlist"), c).andExpect(jsonPath("$", hasSize(1)));
        verify(briefService, times(1)).triggerForTicker("BRK-B");   // 新加的股票马上补一份简报,不用等到明天
        for (int i = 0; i < 19; i++) {
            addTicker("ZQ" + (char) ('A' + i), c).andExpect(status().isOk());
        }
        addTicker("ZQZ", c).andExpect(status().isBadRequest()).andExpect(jsonPath("$.error", containsString("20")));
        addTicker("BRK-B", c).andExpect(status().isOk());   // 已经在列表里的,满了也不报错
    }

    @Test
    void addingATickerStillWorksWhenTheBriefCannotBeStarted() throws Exception {
        Cookie c = register(newEmail());
        when(briefService.triggerForTicker("ZQXF")).thenThrow(new IllegalStateException("kafka down"));
        addTicker("ZQXF", c).andExpect(status().isOk());
        send(get("/api/v1/me/watchlist"), c).andExpect(jsonPath("$", hasSize(1)));
    }

    @Test
    void settingsAreValidatedAndTheWebhookIsNeverEchoedBack() throws Exception {
        Cookie c = register(newEmail());
        send(get("/api/v1/me"), c).andExpect(jsonPath("$.webhook").value(nullValue()));
        json(put("/api/v1/me/settings"),
                "{\"webhookUrl\":\"" + SLACK + "\",\"pushHour\":21,\"timezone\":\"America/Los_Angeles\"}", c)
                .andExpect(status().isOk());
        String me = send(get("/api/v1/me"), c).andExpect(jsonPath("$.pushHour", is(21)))
                .andExpect(jsonPath("$.timezone", is("America/Los_Angeles")))
                .andExpect(jsonPath("$.webhook", is("hooks.slack.com/…WXYZ")))
                .andReturn().getResponse().getContentAsString();
        assertFalse(me.contains("B000"), "完整地址等于发消息的权限,不回显");

        // 只传一项:其它不变
        json(put("/api/v1/me/settings"), "{\"pushHour\":7}", c).andExpect(status().isOk());
        send(get("/api/v1/me"), c).andExpect(jsonPath("$.pushHour", is(7)))
                .andExpect(jsonPath("$.webhook", is("hooks.slack.com/…WXYZ")));

        for (String bad : new String[]{"{\"webhookUrl\":\"https://evil.example/hook\"}",
                "{\"webhookUrl\":\"http://127.0.0.1:8080/api/v1/tasks\"}",
                "{\"pushHour\":24}", "{\"pushHour\":-1}", "{\"timezone\":\"Mars/Phobos\"}"}) {
            json(put("/api/v1/me/settings"), bad, c).andExpect(status().isBadRequest());
        }
        send(get("/api/v1/me"), c).andExpect(jsonPath("$.pushHour", is(7)));   // 被拒的请求什么都没改

        // 空串 = 清除
        json(put("/api/v1/me/settings"), "{\"webhookUrl\":\"\"}", c).andExpect(status().isOk());
        send(get("/api/v1/me"), c).andExpect(jsonPath("$.webhook").value(nullValue()));
    }

    @Test
    void webhookTestNeedsASavedWebhookAndIsLimitedPerHour() throws Exception {
        Cookie c = register(newEmail());
        send(post("/api/v1/me/webhook-test"), c).andExpect(status().isBadRequest());
        json(put("/api/v1/me/settings"), "{\"webhookUrl\":\"" + SLACK + "\"}", c).andExpect(status().isOk());
        for (int i = 0; i < 5; i++) {
            send(post("/api/v1/me/webhook-test"), c).andExpect(status().isOk());
        }
        send(post("/api/v1/me/webhook-test"), c).andExpect(status().isTooManyRequests());
        org.mockito.Mockito.verify(webhookClient, org.mockito.Mockito.times(5))
                .send(org.mockito.ArgumentMatchers.eq(SLACK), org.mockito.ArgumentMatchers.anyString());
    }


    @Test
    void newTickersAreLimitedPerUserPerDay() throws Exception {
        // 反复加了又删会一直触发"新关注立刻整理",那是全站共用的额度:一个账号不能把它耗光
        Cookie c = register(newEmail());
        for (int i = 0; i < 30; i++) {
            addTicker("ZQA", c).andExpect(status().isOk());
            send(delete("/api/v1/me/watchlist").param("ticker", "ZQA"), c).andExpect(status().isOk());
        }
        addTicker("ZQA", c).andExpect(status().isTooManyRequests());
        verify(briefService, times(30)).triggerForTicker("ZQA");
    }
}
