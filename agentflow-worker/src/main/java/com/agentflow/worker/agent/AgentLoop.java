package com.agentflow.worker.agent;

import com.agentflow.common.llm.ChatMessage;
import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import com.agentflow.common.llm.ToolCall;
import com.agentflow.common.llm.ToolSpec;
import com.agentflow.common.trace.TraceStage;
import com.agentflow.worker.agent.tool.AgentTool;
import com.agentflow.worker.llm.LlmGateway;
import com.agentflow.worker.llm.LlmModelProperties;
import com.agentflow.worker.trace.TraceEmitter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * ReAct 循环:思考 → 调工具 → 看结果 → … → 给出带来源的结论。
 *
 * <ul>
 *   <li>每一步完整结束(LLM 决策 + 全部工具结果)后写 {@link AgentCheckpoint};重新投递时从下一步续跑,
 *       已调用过的工具直接复用存档结果,不重复调用。</li>
 *   <li>工具出错不抛:以 "ERROR: ..." 回给模型让它自己纠正;只有 LLM 调用失败才让子任务进入重试。</li>
 *   <li>同一次运行内相同 name+参数的调用走缓存,防止模型原地打转。</li>
 *   <li>最终 sources 只保留本次运行工具真实返回过的 URL(防编造引用),删掉的发 CITATION_DROPPED;
 *       来源附带的原文摘录(quote)必须真的出现在"这个 URL 所在的那段工具结果"里且不少于
 *       {@link #MIN_QUOTE_CHARS} 字,否则去掉摘录,计入 unverifiedQuotes。</li>
 *   <li>最多 {@link #MAX_STEPS} 步,超出则去掉工具强制作答。</li>
 * </ul>
 * ponytail: 上下文靠"单个工具结果截断 2000 字 × 最多 6 步"封顶,没有按 token 预算裁剪历史
 * (tool_call 与结果必须成对,不能像 AgentContext 那样逐条丢);步数调大时再加摘要压缩。
 */
@Slf4j
@Component
public class AgentLoop {

    static final int MAX_STEPS = 6;
    static final int MAX_TOKENS = 1600;   // 800 时带摘录的 JSON 常被截断,来源全丢(见评测 v3)
    static final int PREVIEW_CHARS = 300;
    static final String ERROR = "ERROR: ";
    static final int MIN_QUOTE_CHARS = 20;
    static final String WORKER_ID = hostname();
    private static final ObjectMapper OM = new ObjectMapper();

    private final LlmGateway gateway;
    private final Map<String, AgentTool> tools;
    private final List<ToolSpec> specs;
    private final AgentCheckpoint checkpoint;
    private final TraceEmitter trace;
    private final LlmModelProperties props;

    public AgentLoop(LlmGateway gateway, List<AgentTool> toolList, AgentCheckpoint checkpoint,
                     TraceEmitter trace, LlmModelProperties props) {
        this.gateway = gateway;
        this.tools = toolList.stream().collect(Collectors.toMap(AgentTool::name, Function.identity()));
        this.specs = toolList.stream()
                .map(t -> new ToolSpec(t.name(), t.description(), t.parametersSchema())).toList();
        this.checkpoint = checkpoint;
        this.trace = trace;
        this.props = props;
    }

    public ObjectNode run(AgentRun run) {
        List<ChatMessage> messages = new ArrayList<>();
        messages.add(new ChatMessage("system", Prompts.system(Prompts.AGENT, run.question())));
        messages.add(new ChatMessage("user", run.question()));
        Map<String, String> cache = new HashMap<>();
        Set<String> seenUrls = new LinkedHashSet<>();
        Map<String, StringBuilder> textByUrl = new HashMap<>();   // URL → 它所在的那段工具结果,核对摘录用
        Usage usage = new Usage();

        // 1) 续跑:把已完成的步骤原样放回对话,工具结果进缓存,不重新调用
        List<AgentStep> done = checkpoint.load(run.subtaskUuid());
        for (AgentStep s : done) {
            messages.add(s.assistant());
            messages.addAll(s.toolResults());
            for (int i = 0; i < s.toolResults().size(); i++) {
                String out = s.toolResults().get(i).getContent();
                if (!out.startsWith(ERROR)) {
                    cache.put(cacheKey(s.assistant().getToolCalls().get(i)), out);
                }
                seenUrls.addAll(TextUtil.urls(out));
                indexByUrl(out, textByUrl);
            }
            usage.add(s.usage());
        }
        if (!done.isEmpty()) {
            log.info("agent 从存档续跑 subtaskUuid={} fromStep={}", run.subtaskUuid(), done.size());
            emit(run, TraceStage.RESUMED, OM.createObjectNode()
                    .put("fromStep", done.size()).put("workerId", WORKER_ID));
        }

        // 2) ReAct 主循环
        for (int step = done.size() + 1; step <= MAX_STEPS; step++) {
            ChatResponse resp = gateway.chat(request(run.model(), messages, specs));
            Usage stepUsage = Usage.of(resp, props);
            usage.add(stepUsage);
            if (resp.getToolCalls().isEmpty()) {
                return finish(run, step, resp.getContent(), seenUrls, textByUrl, usage);
            }
            ChatMessage assistant = ChatMessage.assistant(resp.getContent(), resp.getToolCalls());
            List<ChatMessage> results = new ArrayList<>();
            ArrayNode calls = OM.createArrayNode();
            for (ToolCall c : resp.getToolCalls()) {
                String out = execute(c, cache);
                seenUrls.addAll(TextUtil.urls(out));
                indexByUrl(out, textByUrl);
                results.add(ChatMessage.tool(c.getId(), out));
                calls.addObject().put("tool", c.getName()).put("args", c.getArgumentsJson())
                        .put("resultPreview", TextUtil.truncate(out, PREVIEW_CHARS));
            }
            messages.add(assistant);
            messages.addAll(results);
            // 一步完整结束才存档:崩溃在此之前 → 该步重做(工具只读,重做无副作用)
            checkpoint.append(run.subtaskUuid(), new AgentStep(step, assistant, results, stepUsage));
            ObjectNode detail = OM.createObjectNode().put("step", step)
                    .put("thought", resp.getContent()).put("workerId", WORKER_ID);
            detail.set("calls", calls);
            emit(run, TraceStage.AGENT_STEP, detail);
        }

        // 3) 步数耗尽:去掉工具,强制作答(防死循环)
        messages.add(new ChatMessage("user", Prompts.FORCE_ANSWER));
        ChatResponse resp = gateway.chat(request(run.model(), messages, null));
        usage.add(Usage.of(resp, props));
        return finish(run, MAX_STEPS + 1, resp.getContent(), seenUrls, textByUrl, usage);
    }

    private String execute(ToolCall c, Map<String, String> cache) {
        String key = cacheKey(c);
        String hit = cache.get(key);
        if (hit != null) {
            return hit;
        }
        AgentTool tool = tools.get(c.getName());
        if (tool == null) {
            return ERROR + "未知工具 " + c.getName() + ",可用工具:" + tools.keySet();
        }
        String out;
        try {
            String raw = c.getArgumentsJson();
            JsonNode args = OM.readTree(raw == null || raw.isBlank() ? "{}" : raw);
            out = TextUtil.truncate(tool.run(args), TextUtil.MAX_TOOL_CHARS);
        } catch (Exception e) {
            return ERROR + c.getName() + " 调用失败:" + e.getMessage();   // 不缓存:瞬时错误允许模型重试
        }
        cache.put(key, out);
        return out;
    }

    private ObjectNode finish(AgentRun run, int step, String content, Set<String> seenUrls,
                              Map<String, StringBuilder> textByUrl, Usage usage) {
        String answer = content == null ? "" : content.trim();
        JsonNode sources = OM.createArrayNode();
        try {
            JsonNode parsed = TextUtil.parseObject(content);
            if (parsed.path("answer").isTextual()) {
                answer = parsed.get("answer").asText();
                sources = parsed.path("sources");
            }
        } catch (IllegalStateException e) {
            // 模型没按 JSON 作答:整段当答案、无来源。宽松兜底,不为格式问题烧重试。
        }
        if (answer.isBlank()) {
            throw new IllegalStateException("agent 最终答案为空");
        }
        ArrayNode kept = OM.createArrayNode();
        ArrayNode dropped = OM.createArrayNode();
        int unverifiedQuotes = 0;
        for (JsonNode s : sources) {
            String url = s.path("url").asText("");
            if (seenUrls.contains(url)) {
                ObjectNode src = kept.addObject().put("title", s.path("title").asText(url)).put("url", url);
                String quote = s.path("quote").asText("").trim();
                String q = TextUtil.normalize(quote);
                if (q.length() >= MIN_QUOTE_CHARS
                        && TextUtil.normalize(String.valueOf(textByUrl.get(url))).contains(q)) {
                    src.put("quote", quote);
                } else if (!quote.isEmpty()) {
                    unverifiedQuotes++;
                }
            } else {
                dropped.add(url);
            }
        }
        if (!dropped.isEmpty()) {
            ObjectNode d = OM.createObjectNode();
            d.set("urls", dropped);
            emit(run, TraceStage.CITATION_DROPPED, d);
        }
        emit(run, TraceStage.AGENT_STEP, OM.createObjectNode().put("step", step).put("final", true)
                .put("answer", TextUtil.truncate(answer, PREVIEW_CHARS)).put("workerId", WORKER_ID));

        ObjectNode out = OM.createObjectNode();
        out.put("question", run.question());
        out.put("answer", answer);
        out.set("sources", kept);
        out.put("unverifiedQuotes", unverifiedQuotes);
        out.set("usage", OM.valueToTree(usage));
        out.put("steps", step);
        return out;
    }

    /**
     * 工具结果按空行切块(搜索结果一条一块,网页正文整篇一块),块里出现的 URL 关联这块文字。
     * 这样摘录只能来自被引用的那个来源,不能把 A 的原话挂到 B 的链接上。
     */
    static void indexByUrl(String out, Map<String, StringBuilder> textByUrl) {
        for (String block : out.split("\n\\s*\n")) {
            for (String url : TextUtil.urls(block)) {
                textByUrl.computeIfAbsent(url, k -> new StringBuilder()).append(block).append('\n');
            }
        }
    }

    /** 拷贝 messages:后续追加不影响已发出的请求。 */
    private static ChatRequest request(String model, List<ChatMessage> messages, List<ToolSpec> tools) {
        ChatRequest r = new ChatRequest(model, new ArrayList<>(messages), MAX_TOKENS);
        r.setTools(tools);
        return r;
    }

    private static String cacheKey(ToolCall c) {
        return c.getName() + " " + c.getArgumentsJson();
    }

    private void emit(AgentRun run, TraceStage stage, ObjectNode detail) {
        trace.emit(String.valueOf(run.taskId()), run.taskId(), run.subtaskId(), stage, null, detail.toString());
    }

    private static String hostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (Exception e) {
            return "unknown";
        }
    }
}
