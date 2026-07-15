package com.agentflow.worker.skill;

import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * 研究/问答技能:命中 research/资料/对比/什么是 等意图,基于检索到的资料作答。
 * 输出 schema 用 {@code summary}(与 mock/守规矩模型的输出字段一致),ResearchProcessor 再对外暴露为
 * {@code answer}。
 */
@Component
public class RagSearchSkill implements Skill {

    @Override
    public String name() {
        return "rag-search";
    }

    @Override
    public int priority() {
        return 8;
    }

    @Override
    public boolean matches(String intent) {
        if (intent == null) {
            return false;
        }
        String s = intent.toLowerCase();
        return s.contains("research") || s.contains("资料") || s.contains("对比")
                || s.contains("什么") || s.contains("qa") || s.contains("question");
    }

    @Override
    public Map<String, String> outputSchema() {
        return Map.of("summary", "string");
    }

    @Override
    public String systemPrompt() {
        return "你是研究助手。基于给定[资料]简洁作答;只输出 JSON:{\"summary\": \"<答案>\"}。";
    }
}
