package com.agentflow.worker.agent;

import com.agentflow.common.llm.ChatMessage;
import com.agentflow.common.llm.ToolCall;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.redisson.Redisson;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.redisson.config.Config;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/** AgentCheckpoint 集成测试(真 Redis,localhost:6379)。 */
class AgentCheckpointTest {

    private static RedissonClient redisson;
    private static AgentCheckpoint checkpoint;

    @BeforeAll
    static void connect() {
        Config c = new Config();
        c.useSingleServer().setAddress("redis://localhost:6379");
        redisson = Redisson.create(c);
        checkpoint = new AgentCheckpoint(redisson);
    }

    @AfterAll
    static void close() {
        redisson.shutdown();
    }

    private static AgentStep step(int n) {
        ChatMessage assistant = ChatMessage.assistant("think " + n,
                List.of(new ToolCall("call_" + n, "wiki_search", "{\"query\":\"q" + n + "\"}")));
        Usage u = new Usage();
        u.setCalls(1);
        u.setPromptTokens(10);
        return new AgentStep(n, assistant, List.of(ChatMessage.tool("call_" + n, "URL: https://x/" + n)), u);
    }

    @Test
    void appendThenLoadRoundTripsWithTtl() {
        String id = UUID.randomUUID().toString();
        checkpoint.append(id, step(1));
        checkpoint.append(id, step(2));

        List<AgentStep> steps = checkpoint.load(id);
        assertEquals(2, steps.size());
        assertEquals("wiki_search", steps.get(1).assistant().getToolCalls().get(0).getName());
        assertEquals("call_2", steps.get(1).toolResults().get(0).getToolCallId());
        assertEquals(10, steps.get(0).usage().getPromptTokens());
        assertTrue(redisson.getList(AgentCheckpoint.PREFIX + id, StringCodec.INSTANCE).remainTimeToLive() > 0);
    }

    @Test
    void duplicateStepNumbersAreSkipped() {
        String id = UUID.randomUUID().toString();
        checkpoint.append(id, step(1));
        checkpoint.append(id, step(1));   // 并发双跑时可能出现的重复写入
        checkpoint.append(id, step(2));
        assertEquals(List.of(1, 2), checkpoint.load(id).stream().map(AgentStep::step).toList());
    }

    @Test
    void corruptedCheckpointIsDroppedAndStartsFresh() {
        String id = UUID.randomUUID().toString();
        redisson.getList(AgentCheckpoint.PREFIX + id, StringCodec.INSTANCE).add("{not json");
        assertTrue(checkpoint.load(id).isEmpty());
        assertFalse(redisson.getList(AgentCheckpoint.PREFIX + id, StringCodec.INSTANCE).isExists());
    }
}
