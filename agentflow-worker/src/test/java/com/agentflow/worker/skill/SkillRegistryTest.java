package com.agentflow.worker.skill;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SkillRegistryTest {

    private final SkillRegistry registry =
            new SkillRegistry(List.of(new ExtractSkill(), new SummarizeSkill()));

    @Test
    void multiHitOrderedByPriorityDesc() {
        // "text" 两个 skill 都命中 → summarize(优先级 10)排在 extract(5)前
        List<Skill> hits = registry.select("some text task");
        assertEquals(2, hits.size());
        assertEquals("summarize", hits.get(0).name());
        assertEquals("extract", hits.get(1).name());
    }

    @Test
    void selectBestReturnsHighestPriority() {
        assertEquals("summarize", registry.selectBest("text").orElseThrow().name());
    }

    @Test
    void nonMatchingIntentReturnsEmpty() {
        assertTrue(registry.select("翻译").isEmpty());
        assertTrue(registry.selectBest("翻译").isEmpty());
    }

    @Test
    void summarizeOnlyForSummarizeIntent() {
        List<Skill> hits = registry.select("summarize");
        assertEquals(1, hits.size());
        assertEquals("summarize", hits.get(0).name());
    }
}
