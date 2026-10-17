package com.agentflow.worker.agent;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/** 提示词是中文写的,不显式指定语言时模型会用中文回答英文问题(评测里 lang_match 只有 50%)。 */
class PromptsTest {

    @Test
    void englishQuestionGetsEnglishInstruction() {
        String sys = Prompts.system(Prompts.AGENT, "How does Raft elect a leader?");
        assertTrue(sys.startsWith(Prompts.AGENT_TAG), "角色标签必须仍在开头(mock 靠它识别角色)");
        assertTrue(sys.contains("Today is "), sys);
        assertTrue(sys.endsWith(Prompts.ENGLISH), sys);
    }

    @Test
    void chineseQuestionGetsChineseInstruction() {
        assertTrue(Prompts.system(Prompts.PLANNER, "Redis 的持久化机制 RDB 和 AOF 有什么区别？").endsWith(Prompts.CHINESE));
        // 中文问题里夹英文术语仍是中文问题
        assertTrue(Prompts.system(Prompts.PLANNER, "LangGraph 和 CrewAI 有什么区别").endsWith(Prompts.CHINESE));
    }
}
