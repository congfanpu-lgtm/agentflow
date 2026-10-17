package com.agentflow.server.demo;

import org.junit.jupiter.api.Test;
import org.redisson.api.RedissonClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.time.ZoneOffset;

import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

/** 公开演示保护:访问码、每 IP 每日次数、每日花费上限、只允许研究任务。 */
@SpringBootTest(properties = {"demo.access-code=s3cret", "demo.per-ip-daily=2", "demo.daily-budget-usd=1000"})
@AutoConfigureMockMvc
@Transactional
class DemoGuardTest {

    private static final String RESEARCH = "{\"type\":\"RESEARCH_AGENT\",\"payload\":{\"question\":\"kafka?\"}}";

    @Autowired
    private MockMvc mvc;
    @Autowired
    private RedissonClient redisson;
    @MockBean
    private KafkaTemplate<String, Object> kafkaTemplate;

    /** 每个用例用不同的客户端 IP,互不占用配额。 */
    private final String ip = "10.77." + (int) (Math.random() * 250) + "." + (int) (Math.random() * 250);

    private ResultActions submit(String body, String code) throws Exception {
        var req = post("/api/v1/tasks").contentType(MediaType.APPLICATION_JSON).content(body)
                .with(r -> { r.setRemoteAddr(ip); return r; });
        return mvc.perform(code == null ? req : req.header("X-Access-Code", code));
    }

    @Test
    void apiNeedsTheAccessCodeButThePageDoesNot() throws Exception {
        mvc.perform(get("/api/v1/llm/usage")).andExpect(status().isUnauthorized())
                .andExpect(jsonPath("$.error", containsString("access code")));
        mvc.perform(get("/api/v1/llm/usage").header("X-Access-Code", "wrong")).andExpect(status().isUnauthorized());
        mvc.perform(get("/api/v1/llm/usage").header("X-Access-Code", "s3cret")).andExpect(status().isOk());
        mvc.perform(get("/")).andExpect(status().isOk());
        submit(RESEARCH, null).andExpect(status().isUnauthorized());
    }

    @Test
    void perIpDailyLimit() throws Exception {
        submit(RESEARCH, "s3cret").andExpect(status().isAccepted());
        submit(RESEARCH, "s3cret").andExpect(status().isAccepted());
        submit(RESEARCH, "s3cret").andExpect(status().isTooManyRequests())
                .andExpect(jsonPath("$.error", containsString("2 research tasks per day")));
    }

    @Test
    void dailyBudgetStopsNewTasks() throws Exception {
        var day = redisson.getAtomicLong("agentflow:cost:day:" + LocalDate.now(ZoneOffset.UTC));
        long add = 1_000_000_001;   // 当天已花 > $1000(预算设大,真实花费不影响其他用例)
        day.addAndGet(add);
        try {
            submit(RESEARCH, "s3cret").andExpect(status().isTooManyRequests())
                    .andExpect(jsonPath("$.error", containsString("budget")));
        } finally {
            day.addAndGet(-add);
        }
    }

    @Test
    void onlyResearchTasksAndNoModelOverride() throws Exception {
        // 演示模式下不能提交 LLM_BATCH 这类可批量烧钱的任务,也不能指定更贵的模型
        submit("{\"type\":\"LLM_BATCH\",\"payload\":{\"items\":[\"a\"]}}", "s3cret").andExpect(status().isForbidden());
        submit("{\"type\":\"RESEARCH_AGENT\",\"payload\":{\"question\":\"q\",\"model\":\"big\"}}", "s3cret")
                .andExpect(status().isForbidden());
    }

    @Test
    void clientIpTrustsProxyHeaderOnlyWhenConfigured() {
        MockHttpServletRequest r = new MockHttpServletRequest();
        r.setRemoteAddr("127.0.0.1");
        r.addHeader("CF-Connecting-IP", "203.0.113.9");
        assertEquals("127.0.0.1", DemoGuard.clientIp(r, false), "不信任时客户端可伪造这个头");
        assertEquals("203.0.113.9", DemoGuard.clientIp(r, true));
    }
}
