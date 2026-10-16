package com.agentflow.worker.llm;

import com.agentflow.common.llm.ChatMessage;
import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import com.agentflow.common.llm.TokenUsage;
import com.agentflow.common.llm.ToolCall;
import com.agentflow.worker.agent.Prompts;
import com.agentflow.worker.agent.TextUtil;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * 默认 LLM 后端:确定性输出、不发网络、无需 API key——压测测调度不烧钱(master 设计文档第 149 行)。
 * 网关的限流/路由/记账对 mock 与真实模型完全一致,mock 只是把"发 HTTP"换成"本地确定性返回"。
 *
 * <p>RESEARCH_AGENT 剧本:按系统提示的角色标签({@link Prompts})分流——
 * planner 固定拆 3 个子问题;agent 第 1 步 wiki_search、第 2 步 web_fetch、第 3 步给答案;
 * synthesizer 返回引用 [1] 的报告。其余请求保持原有"摘要"行为。
 * {@code llm.mock.delay-ms} 模拟模型耗时,便于在页面上看清每一步、演示中途杀 worker。
 */
@Component
@ConditionalOnProperty(name = "llm.provider", havingValue = "mock", matchIfMissing = true)
public class MockLlmClient implements LlmClient {

    private static final ObjectMapper OM = new ObjectMapper();
    private static final Set<String> STOP = Set.of("what", "which", "does", "with", "from", "that", "this",
            "between", "about", "versus", "when", "where", "there", "their", "have");

    @Value("${llm.mock.delay-ms:0}")
    private long delayMs;

    @Override
    public String provider() {
        return "mock";
    }

    @Override
    public ChatResponse chat(ChatRequest request) {
        sleep();
        List<ChatMessage> msgs = request.getMessages() == null ? List.of() : request.getMessages();
        String system = msgs.stream().filter(m -> "system".equals(m.getRole()) && m.getContent() != null)
                .map(ChatMessage::getContent).findFirst().orElse("");
        ChatResponse resp;
        if (system.startsWith(Prompts.PLANNER_TAG)) {
            resp = plan(firstUser(msgs));
        } else if (system.startsWith(Prompts.AGENT_TAG)) {
            resp = agentStep(request, msgs);
        } else if (system.startsWith(Prompts.SYNTHESIZER_TAG)) {
            resp = synthesize(lastUser(msgs));
        } else {
            resp = summarize(lastUser(msgs));
        }
        // token 估算:chars/4(见 TokenEstimator 同款启发式);mock 无真实 usage 故本地估。
        int promptChars = msgs.stream().mapToInt(m -> m.getContent() == null ? 0 : m.getContent().length()).sum();
        int completionChars = resp.getContent() == null ? 0 : resp.getContent().length();
        resp.setUsage(new TokenUsage(Math.max(1, promptChars / 4), Math.max(1, completionChars / 4)));
        resp.setModel(request.getModel());
        return resp;
    }

    // 模拟"守规矩的模型":确定性输出符合 summarize schema 的 JSON——processor 会真的解析+校验它。
    private ChatResponse summarize(String lastUser) {
        String trimmed = lastUser.length() > 120 ? lastUser.substring(0, 120) : lastUser;
        return new ChatResponse(OM.createObjectNode().put("summary", "SUMMARY: " + trimmed.trim()).toString(),
                null, null);
    }

    private ChatResponse plan(String question) {
        ObjectNode out = OM.createObjectNode();
        out.putArray("subQuestions")
                .add(question + " — 背景与定义")
                .add(question + " — 关键取舍")
                .add(question + " — 实际案例");
        return new ChatResponse(out.toString(), null, null);
    }

    private ChatResponse agentStep(ChatRequest req, List<ChatMessage> msgs) {
        String question = firstUser(msgs);
        List<String> toolOutputs = msgs.stream().filter(m -> "tool".equals(m.getRole()))
                .map(ChatMessage::getContent).toList();
        boolean toolsOffered = req.getTools() != null && !req.getTools().isEmpty();
        if (toolsOffered && toolOutputs.isEmpty()) {
            return withCall("MOCK 思路:先在 Wikipedia 搜索关键词。",
                    new ToolCall("call_1", "wiki_search", OM.createObjectNode().put("query", keywords(question)).toString()));
        }
        if (toolsOffered && toolOutputs.size() == 1) {
            String url = TextUtil.urls(toolOutputs.get(0)).stream()
                    .filter(u -> u.startsWith("http")).findFirst().orElse(null);
            if (url != null) {
                return withCall("MOCK 思路:打开第一条结果阅读正文。",
                        new ToolCall("call_2", "web_fetch", OM.createObjectNode().put("url", url).toString()));
            }
        }
        List<String> urls = toolOutputs.stream().flatMap(o -> TextUtil.urls(o).stream())
                .distinct().limit(2).toList();
        ObjectNode ans = OM.createObjectNode().put("answer",
                "MOCK 结论:关于「" + question + "」,共查阅 " + toolOutputs.size() + " 份工具结果。");
        ArrayNode src = ans.putArray("sources");
        for (int i = 0; i < urls.size(); i++) {
            src.addObject().put("title", "Source " + (i + 1)).put("url", urls.get(i));
        }
        return new ChatResponse(ans.toString(), null, null);
    }

    private ChatResponse synthesize(String user) {
        boolean hasSource = user.contains("\n[1] ");
        return new ChatResponse("MOCK 报告:以下综合了各子问题的结论" + (hasSource ? " [1]" : "") + "。\n\n"
                + "(mock 模式不调用真实模型;配置 LLM_PROVIDER=openai 后这里是真实报告。)", null, null);
    }

    /**
     * 剧本里的"模型"会提炼搜索关键词:整句问题直接丢给 Wikipedia 全文搜索几乎搜不到结果。
     * 优先取句中首字母大写的专有名词(Kafka、RabbitMQ),否则取前 3 个非停用词。
     */
    static String keywords(String question) {
        String q = question.replaceAll("\\s+—.*$", "");
        String[] words = q.split("[^\\p{L}\\p{N}-]+");
        List<String> caps = Arrays.stream(words).skip(1)
                .filter(w -> !w.isEmpty() && Character.isUpperCase(w.charAt(0))).toList();
        if (!caps.isEmpty()) {
            return String.join(" ", caps);
        }
        List<String> plain = Arrays.stream(words)
                .filter(w -> w.length() > 3 && !STOP.contains(w.toLowerCase(Locale.ROOT))).limit(3).toList();
        return plain.isEmpty() ? q : String.join(" ", plain);
    }

    private static ChatResponse withCall(String thought, ToolCall call) {
        ChatResponse r = new ChatResponse(thought, null, null);
        r.setToolCalls(List.of(call));
        return r;
    }

    private static String firstUser(List<ChatMessage> msgs) {
        return msgs.stream().filter(m -> "user".equals(m.getRole()) && m.getContent() != null)
                .map(ChatMessage::getContent).findFirst().orElse("");
    }

    private static String lastUser(List<ChatMessage> msgs) {
        String last = "";
        for (ChatMessage m : msgs) {
            if ("user".equals(m.getRole()) && m.getContent() != null) {
                last = m.getContent();
            }
        }
        return last;
    }

    private void sleep() {
        if (delayMs <= 0) {
            return;
        }
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
