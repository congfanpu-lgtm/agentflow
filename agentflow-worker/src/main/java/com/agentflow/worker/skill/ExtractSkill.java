package com.agentflow.worker.skill;

import org.springframework.stereotype.Component;

import java.util.Map;

/** 实体抽取技能:与 summarize 共同命中 "text" 意图,用于演示多命中排序(优先级更低)。 */
@Component
public class ExtractSkill implements Skill {

    @Override
    public String name() {
        return "extract";
    }

    @Override
    public int priority() {
        return 5;
    }

    @Override
    public boolean matches(String intent) {
        if (intent == null) {
            return false;
        }
        String s = intent.toLowerCase();
        return s.contains("extract") || s.contains("抽取") || s.contains("text");
    }

    @Override
    public Map<String, String> outputSchema() {
        return Map.of("entities", "array");
    }

    @Override
    public String systemPrompt() {
        return "你是实体抽取助手。只输出 JSON:{\"entities\": [\"<实体>\", ...]}。";
    }
}
