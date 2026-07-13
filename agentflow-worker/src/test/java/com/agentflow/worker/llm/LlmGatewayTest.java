package com.agentflow.worker.llm;

import com.agentflow.common.llm.ChatMessage;
import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import com.agentflow.common.llm.TokenUsage;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.config.Config;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** 网关集成测试(真 Redis + mock client):验证限流打爆抛异常 + token 记账累加。 */
class LlmGatewayTest {

    private static RedissonClient redisson;

    @BeforeAll
    static void up() {
        Config cfg = new Config();
        cfg.useSingleServer().setAddress("redis://localhost:6379");
        redisson = Redisson.create(cfg);
    }

    @AfterAll
    static void down() {
        redisson.shutdown();
    }

    private ChatRequest req(String model) {
        return new ChatRequest(model, List.of(new ChatMessage("user", "hi")), 64);
    }

    private LlmGateway gateway(String model, int capacity) {
        LlmModelProperties props = new LlmModelProperties();
        LlmModelProperties.Model m = new LlmModelProperties.Model();
        m.setName(model);
        m.setCapacity(capacity);
        m.setRefillPerSec(0);
        m.setPricePer1kTokens(1.0);
        props.setModels(List.of(m));
        props.setDefaultModel(model);
        LlmClient mock = new LlmClient() {
            public String provider() { return "mock"; }
            public ChatResponse chat(ChatRequest r) {
                return new ChatResponse("ok", new TokenUsage(10, 20), r.getModel());
            }
        };
        return new LlmGateway(props, new RateLimiter(redisson), new TokenAccountant(redisson), mock);
    }

    @Test
    void rateLimitedAfterBucketExhausted() {
        String model = "gw-" + UUID.randomUUID();
        LlmGateway gw = gateway(model, 2);
        gw.chat(req(model));
        gw.chat(req(model));
        assertThrows(RateLimitedException.class, () -> gw.chat(req(model)));
    }

    @Test
    void accountsTokensPerCall() {
        String model = "gw-" + UUID.randomUUID();
        LlmGateway gw = gateway(model, 100);
        gw.chat(req(model));
        gw.chat(req(model));
        TokenAccountant acct = new TokenAccountant(redisson);
        assertEquals(20, acct.promptTokens(model));       // 10 * 2
        assertEquals(40, acct.completionTokens(model));    // 20 * 2
    }

    @Test
    void resolvesDefaultModelWhenRequestModelNull() {
        String model = "gw-" + UUID.randomUUID();
        LlmGateway gw = gateway(model, 5);
        ChatResponse resp = gw.chat(req(null));           // model=null → 用 defaultModel
        assertEquals(model, resp.getModel());
    }
}
