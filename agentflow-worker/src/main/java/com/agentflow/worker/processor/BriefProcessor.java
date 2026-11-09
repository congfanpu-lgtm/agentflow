package com.agentflow.worker.processor;

import com.agentflow.common.llm.ChatMessage;
import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import com.agentflow.worker.agent.Prompts;
import com.agentflow.worker.agent.TextUtil;
import com.agentflow.worker.agent.Usage;
import com.agentflow.worker.agent.tool.WebSearchTool;
import com.agentflow.worker.brief.SecFilings;
import com.agentflow.worker.llm.LlmGateway;
import com.agentflow.worker.llm.LlmModelProperties;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 每日简报的一个子任务:一只股票。取 SEC 公告 + 当天新闻 → 去掉已经报道过的链接 → 模型挑出实质性事件。
 *
 * <p>公告和新闻是外部文本,可能夹带"忽略以上指令"之类的内容,所以不信模型的输出:
 * 事件只能用编号指向我们给它的条目(链接取自条目,模型给的链接一律不用),引文要能在条目原文里逐字找到。
 * 两个来源都失败才算子任务失败(引擎会重试);只挂一个就带着 warning 继续。
 */
@Component
@RequiredArgsConstructor
public class BriefProcessor implements SubtaskProcessor {

    static final int MAX_ITEMS = 10;
    private static final int ITEM_CHARS = 5000;
    private static final ObjectMapper OM = new ObjectMapper();

    private final LlmGateway gateway;
    private final LlmModelProperties props;
    private final WebSearchTool search;
    private final SecFilings sec;

    /** 给模型看的一条材料。form / date 只有 SEC 公告才有。 */
    private record Item(String title, String url, String text, String form, String date) {}

    @Override
    public String type() {
        return "BRIEF";
    }

    @Override
    public String process(String inputJson) throws Exception {
        JsonNode in = OM.readTree(inputJson);
        String ticker = in.path("ticker").asText("").trim();
        if (ticker.isEmpty()) {
            throw new IllegalArgumentException("ticker 不能为空");
        }
        LocalDate date;
        try {
            date = LocalDate.parse(in.path("date").asText(""));
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("date 必须是 YYYY-MM-DD");
        }
        String name = in.path("name").asText("").trim().isEmpty() ? ticker : in.path("name").asText().trim();
        List<String> warnings = new ArrayList<>();
        List<Item> items = collect(ticker, name, in.path("cik").asText(""), date, in.path("seenUrls"), warnings);

        ObjectNode out = OM.createObjectNode().put("ticker", ticker).put("date", date.toString());
        ArrayNode events = out.putArray("events");
        Usage usage = new Usage();
        if (!items.isEmpty()) {
            String model = in.hasNonNull("model") ? in.get("model").asText() : null;
            ChatResponse resp = gateway.chat(new ChatRequest(model, List.of(
                    new ChatMessage("system", Prompts.BRIEFER),
                    new ChatMessage("user", userMessage(ticker, name, date, in.path("knownEvents"), items))), 1200));
            usage = Usage.of(resp, props);
            addChecked(events, TextUtil.parseObject(resp.getContent()).path("events"), items);
        }
        ArrayNode w = out.putArray("warnings");
        warnings.forEach(w::add);
        out.set("usage", OM.valueToTree(usage));
        return out.toString();
    }

    private List<Item> collect(String ticker, String name, String cik, LocalDate date, JsonNode seenUrls,
                               List<String> warnings) throws InterruptedException {
        Map<String, Item> byUrl = new LinkedHashMap<>();   // 公告排在新闻前面:一手来源优先占名额
        int attempted = 1;
        if (!cik.isEmpty() && sec.enabled()) {
            attempted++;
            try {
                for (SecFilings.Filing f : sec.recent(cik, date.minusDays(3))) {
                    byUrl.putIfAbsent(f.url(), new Item(name + " " + f.title(), f.url(), f.text(), f.form(), f.date()));
                }
            } catch (InterruptedException e) {
                throw e;
            } catch (Exception e) {
                warnings.add("SEC 公告获取失败:" + e.getMessage());
            }
        }
        try {
            String query = (name.equalsIgnoreCase(ticker) ? ticker : name + " " + ticker) + " stock news";
            for (WebSearchTool.Hit h : search.search(query, "day")) {
                byUrl.putIfAbsent(h.url(), new Item(oneLine(h.title()), h.url(), h.snippet(), null, null));
            }
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            warnings.add("新闻搜索失败:" + e.getMessage());
        }
        if (warnings.size() == attempted) {
            throw new IllegalStateException(String.join(";", warnings));
        }
        Set<String> seen = new HashSet<>();
        seenUrls.forEach(u -> seen.add(u.asText()));
        return byUrl.values().stream()
                .filter(i -> i.url().startsWith("http") && !seen.contains(i.url()))
                .limit(MAX_ITEMS).toList();
    }

    private static String userMessage(String ticker, String name, LocalDate date, JsonNode knownEvents, List<Item> items) {
        StringBuilder sb = new StringBuilder("公司:").append(name).append(" (").append(ticker).append(")\n")
                .append("日期:").append(date).append('\n');
        if (!knownEvents.isEmpty()) {
            sb.append("已报道事件:\n");
            for (int i = 0; i < knownEvents.size() && i < 20; i++) {
                sb.append("- ").append(TextUtil.truncate(oneLine(knownEvents.get(i).asText()), 100)).append('\n');
            }
        }
        sb.append("条目:\n");
        for (int i = 0; i < items.size(); i++) {
            Item it = items.get(i);
            sb.append('[').append(i + 1).append("] ").append(it.title()).append('\n')
                    .append("URL: ").append(it.url()).append('\n')
                    .append("正文: ").append(TextUtil.truncate(oneLine(it.text()), ITEM_CHARS)).append("\n\n");
        }
        return sb.toString();
    }

    /** 只留下站得住的事件:编号指向真实条目、每个条目最多一个事件、有标题和摘要。 */
    private static void addChecked(ArrayNode events, JsonNode proposed, List<Item> items) {
        Set<Integer> used = new HashSet<>();
        for (JsonNode e : proposed) {
            int idx = e.path("source").isInt() ? e.path("source").asInt() : 0;
            String title = e.path("title").asText("").trim();
            String summary = e.path("summary").asText("").trim();
            if (idx < 1 || idx > items.size() || title.isEmpty() || summary.isEmpty() || !used.add(idx)) {
                continue;
            }
            Item it = items.get(idx - 1);
            ObjectNode ev = events.addObject()
                    .put("title", TextUtil.truncate(title, 100))
                    .put("summary", TextUtil.truncate(summary, 500))
                    .put("importance", Math.max(1, Math.min(3, e.path("importance").asInt(2))))
                    .put("url", it.url())
                    .put("sourceTitle", TextUtil.truncate(it.title(), 200));
            String quote = e.path("quote").asText("").trim();
            if (!quote.isEmpty()
                    && TextUtil.normalize(it.title() + " " + it.text()).contains(TextUtil.normalize(quote))) {
                ev.put("quote", TextUtil.truncate(quote, 500));
            }
            if (it.form() != null) {
                ev.put("form", it.form()).put("eventDate", it.date());
            }
        }
    }

    private static String oneLine(String s) {
        return s == null ? "" : s.replaceAll("\\s+", " ").trim();
    }
}
