package com.agentflow.worker.agent;

import com.agentflow.common.llm.ChatResponse;
import com.agentflow.common.llm.TokenUsage;
import com.agentflow.worker.llm.LlmModelProperties;
import lombok.Getter;
import lombok.Setter;

/**
 * 一次子任务处理内所有 LLM 调用的 token / 成本累计,随输出返回给页面页脚。
 * 成本以微美元(long)累加,与 TokenAccountant 同口径,避免浮点漂移。
 */
@Getter
@Setter
public class Usage {
    private int calls;
    private long promptTokens;
    private long completionTokens;
    private long costMicro;

    public static Usage of(ChatResponse resp, LlmModelProperties props) {
        Usage u = new Usage();
        if (resp == null || resp.getUsage() == null) {
            return u;
        }
        TokenUsage t = resp.getUsage();
        u.calls = 1;
        u.promptTokens = t.getPromptTokens();
        u.completionTokens = t.getCompletionTokens();
        u.costMicro = Math.round(t.total() / 1000.0
                * props.model(resp.getModel()).getPricePer1kTokens() * 1_000_000);
        return u;
    }

    public void add(Usage o) {
        if (o == null) {
            return;
        }
        calls += o.calls;
        promptTokens += o.promptTokens;
        completionTokens += o.completionTokens;
        costMicro += o.costMicro;
    }
}
