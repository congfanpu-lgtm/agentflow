package com.agentflow.worker.context;

import com.agentflow.common.llm.ChatMessage;
import com.agentflow.worker.llm.LlmModelProperties;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class AgentContextTest {

    private AgentContext ctx(int maxTokens) {
        LlmModelProperties props = new LlmModelProperties();
        props.setMaxContextTokens(maxTokens);
        return new AgentContext(new TokenEstimator(), props);
    }

    @Test
    void underBudgetKeepsEverything() {
        List<ChatMessage> hist = new ArrayList<>(List.of(
                new ChatMessage("user", "q1"), new ChatMessage("assistant", "a1")));
        List<ChatMessage> out = ctx(1000).build("sys", "hello", hist);
        assertEquals("system", out.get(0).getRole());
        assertEquals(4, out.size());                       // system + 2 history + user
        assertEquals("hello", out.get(3).getContent());
    }

    @Test
    void dropsOldestHistoryWhenOverBudget() {
        List<ChatMessage> hist = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            hist.add(new ChatMessage("user", "history line number " + i + " with some words"));
        }
        List<ChatMessage> out = ctx(20).build("sys", "current", hist);
        // 预算很小 → 大部分历史被丢弃;system + user 一定在,历史被裁到很少
        assertEquals("system", out.get(0).getRole());
        assertEquals("user", out.get(out.size() - 1).getRole());
        assertEquals("current", out.get(out.size() - 1).getContent());
        assertTrue(out.size() < hist.size(), "history should be trimmed, size=" + out.size());
    }

    @Test
    void truncatesOverlongInputWhenNoHistoryLeft() {
        String huge = "x".repeat(10000);
        List<ChatMessage> out = ctx(50).build("sys", huge, List.of());
        String userContent = out.get(out.size() - 1).getContent();
        assertTrue(userContent.length() < huge.length(), "should truncate");
        assertTrue(userContent.contains("裁剪"), "should mark truncation");
    }
}
