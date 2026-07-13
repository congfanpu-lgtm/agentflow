package com.agentflow.worker.context;

import com.agentflow.common.llm.ChatMessage;
import com.agentflow.worker.llm.LlmModelProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 上下文注入 + 防膨胀(腾一面 Q5/Q6:"上下文注入怎么做""如何避免过多历史进模型")。
 *
 * <p>组装 system + 历史 + 当前输入,按 token 预算裁剪:超预算时**优先丢最老历史**,历史丢光仍超
 * 则**截断超长输入**(保留头尾),绝不无上限塞给模型。裁剪量记日志(可观测)。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AgentContext {

    private final TokenEstimator estimator;
    private final LlmModelProperties props;

    /** history 传空即单轮(本里程碑 LLM_BATCH 单轮;裁剪对超长单条 input 同样生效)。 */
    public List<ChatMessage> build(String systemPrompt, String userInput, List<ChatMessage> history) {
        int budget = props.getMaxContextTokens();
        int systemTokens = estimator.estimate(systemPrompt);

        // 1) 历史从最老开始丢,直到 system+history+input 估算 <= budget(或历史丢光)
        List<ChatMessage> kept = new ArrayList<>(history == null ? List.of() : history);
        int inputTokens = estimator.estimate(userInput);
        int dropped = 0;
        while (!kept.isEmpty()
                && systemTokens + tokensOf(kept) + inputTokens > budget) {
            kept.remove(0);
            dropped++;
        }

        // 2) 历史清空后仍超:截断当前输入(保留头尾)
        String finalInput = userInput;
        int remain = budget - systemTokens - tokensOf(kept);
        if (inputTokens > remain) {
            finalInput = truncate(userInput, Math.max(0, remain));
            log.info("上下文防膨胀:input 从 ~{} token 截断到 ~{} token(预算 {})",
                    inputTokens, estimator.estimate(finalInput), budget);
        }
        if (dropped > 0) {
            log.info("上下文防膨胀:丢弃 {} 条最老历史以守住 {} token 预算", dropped, budget);
        }

        List<ChatMessage> out = new ArrayList<>();
        if (systemPrompt != null && !systemPrompt.isBlank()) {
            out.add(new ChatMessage("system", systemPrompt));
        }
        out.addAll(kept);
        out.add(new ChatMessage("user", finalInput));
        return out;
    }

    private int tokensOf(List<ChatMessage> msgs) {
        int t = 0;
        for (ChatMessage m : msgs) {
            t += estimator.estimate(m.getContent());
        }
        return t;
    }

    /** 保留头尾、中间省略,粗按 4 字/token 反算字符预算。 */
    private String truncate(String text, int tokenBudget) {
        int targetChars = tokenBudget * 4;
        if (text.length() <= targetChars) {
            return text;
        }
        if (targetChars < 8) {   // 预算太小:只留个头
            return text.substring(0, Math.max(0, targetChars));
        }
        int head = targetChars * 2 / 3;
        int tail = targetChars - head;
        int cut = text.length() - targetChars;
        return text.substring(0, head) + "…[裁剪" + cut + "字防上下文膨胀]…"
                + text.substring(text.length() - tail);
    }
}
