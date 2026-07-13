package com.agentflow.worker.skill;

import org.springframework.stereotype.Component;

import java.util.Map;

/** 摘要技能:LLM_BATCH 默认技能。命中 summarize/摘要/text 意图,优先级高。 */
@Component
public class SummarizeSkill implements Skill {

    @Override
    public String name() {
        return "summarize";
    }

    @Override
    public int priority() {
        return 10;
    }

    @Override
    public boolean matches(String intent) {
        if (intent == null) {
            return false;
        }
        String s = intent.toLowerCase();
        return s.contains("summar") || s.contains("摘要") || s.contains("text");
    }

    @Override
    public Map<String, String> outputSchema() {
        return Map.of("summary", "string");
    }

    @Override
    public String systemPrompt() {
        return "你是文本摘要助手。只输出 JSON:{\"summary\": \"<不超过一句话的摘要>\"}。";
    }
}
