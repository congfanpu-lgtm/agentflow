package com.agentflow.server.coordinator;

import com.agentflow.common.state.SubtaskStatus;
import com.agentflow.server.entity.StockEventEntity;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.entity.WatchlistEntity;
import com.agentflow.server.mapper.StockEventMapper;
import com.agentflow.server.mapper.WatchlistMapper;
import com.agentflow.server.product.BriefService;
import com.agentflow.server.product.TickerDirectory;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * DAILY_BRIEF:每只被关注的股票拆成一个 BRIEF 子任务(fan-out),worker 并行去查公告和新闻;
 * 全部落定后 {@link #onFinished} 把各子任务产出的事件写进 stock_event。
 *
 * <p>payload:{@code {"tickers":["AAPL"], "date":"2026-11-09"}},两个字段都可省略。
 * 股票永远来自 watchlist 表——payload 里的 tickers 只能把范围缩小,不能让系统去查没人关注的股票。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class DailyBriefDecomposer implements TaskDecomposer {

    public static final String TYPE = "DAILY_BRIEF";
    private static final Pattern TICKER = Pattern.compile("^[A-Z]{1,5}(-[A-Z]{1,2})?$");
    private static final int MAX_SEEN_URLS = 200;
    private static final int MAX_KNOWN_EVENTS = 20;

    private final ObjectMapper objectMapper;
    private final WatchlistMapper watchlistMapper;
    private final StockEventMapper stockEventMapper;
    private final TickerDirectory tickerDirectory;

    // ponytail: 一次任务最多 100 只(按关注人数取前 100)。上限来自超时巡检:子任务发出 600 秒没结果会被重投,
    // 4 个 worker 并发、每只约 20 秒时 100 只大约排 8 分钟,再多就会被当成卡死而重复执行。
    // 股票更多时:分批提交 / 加 worker / 调大 agentflow.timeout.stuck-seconds。
    @Value("${agentflow.brief.max-tickers:100}")
    private int maxTickers;

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public List<SubtaskDef> decompose(JsonNode payload) {
        LocalDate date = date(payload);
        List<String> only = requestedTickers(payload);

        QueryWrapper<WatchlistEntity> q = new QueryWrapper<WatchlistEntity>()
                .select("ticker", "MAX(company_name) AS name", "COUNT(*) AS watchers");
        if (only != null) {
            q.in("ticker", only);
        }
        List<Map<String, Object>> watched = watchlistMapper.selectMaps(
                // 关注人数相同的按日期轮换(不是按字母):超过上限时,排在后面的股票不会永远轮不到。date 是解析过的 LocalDate
                q.groupBy("ticker").last("ORDER BY watchers DESC, CRC32(CONCAT(ticker, '" + date + "')), ticker LIMIT " + maxTickers));
        if (watched.isEmpty()) {
            throw new IllegalArgumentException("没有需要生成简报的股票");
        }
        List<String> tickers = watched.stream().map(m -> String.valueOf(m.get("ticker"))).toList();

        // 最近 7 天已经报过的:出处网址(worker 直接跳过)和标题(交给模型判断"是不是同一件事")
        Map<String, Set<String>> seenUrls = new HashMap<>();
        Map<String, List<String>> knownEvents = new HashMap<>();
        for (StockEventEntity e : stockEventMapper.selectList(new QueryWrapper<StockEventEntity>()
                .select("ticker", "url", "title").in("ticker", tickers)
                .apply("created_at >= NOW() - INTERVAL 7 DAY").orderByDesc("id").last("LIMIT 5000"))) {
            Set<String> urls = seenUrls.computeIfAbsent(e.getTicker(), k -> new LinkedHashSet<>());
            if (urls.size() < MAX_SEEN_URLS) {
                urls.add(e.getUrl());
            }
            List<String> titles = knownEvents.computeIfAbsent(e.getTicker(), k -> new ArrayList<>());
            if (titles.size() < MAX_KNOWN_EVENTS) {
                titles.add(e.getTitle());
            }
        }

        List<SubtaskDef> defs = new ArrayList<>();
        for (Map<String, Object> row : watched) {
            String ticker = String.valueOf(row.get("ticker"));
            ObjectNode in = objectMapper.createObjectNode();
            in.put("ticker", ticker);
            // 目录第一次用时要联网拉一次(之后内存缓存 24 小时);拿不到 CIK 就不带,worker 只用新闻
            String cik = tickerDirectory.find(ticker).map(TickerDirectory.Company::cik).orElse(null);
            if (cik != null) {
                in.put("cik", cik);
            }
            in.put("name", row.get("name") == null ? ticker : String.valueOf(row.get("name")));
            in.put("date", date.toString());
            ArrayNode urls = in.putArray("seenUrls");
            seenUrls.getOrDefault(ticker, Set.of()).forEach(urls::add);
            ArrayNode titles = in.putArray("knownEvents");
            knownEvents.getOrDefault(ticker, List.of()).forEach(titles::add);
            defs.add(new SubtaskDef(defs.size(), in.toString(), "BRIEF"));
        }
        return defs;
    }

    private static LocalDate date(JsonNode payload) {
        JsonNode d = payload == null ? null : payload.get("date");
        if (d == null || d.isNull()) {
            return LocalDate.now(BriefService.MARKET_ZONE);
        }
        try {
            return LocalDate.parse(d.asText());
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("date 必须是 YYYY-MM-DD");
        }
    }

    /** null = 没指定(全部自选股)。 */
    private static List<String> requestedTickers(JsonNode payload) {
        JsonNode t = payload == null ? null : payload.get("tickers");
        if (t == null || t.isNull()) {
            return null;
        }
        if (!t.isArray() || t.isEmpty()) {
            throw new IllegalArgumentException("tickers 必须是非空数组");
        }
        List<String> out = new ArrayList<>();
        for (JsonNode n : t) {
            if (!n.isTextual() || !TICKER.matcher(n.asText()).matches()) {
                throw new IllegalArgumentException("股票代码格式不对: " + n);
            }
            out.add(n.asText());
        }
        return out;
    }

    /**
     * 任务落定后保存事件。worker 的输出当作不可信数据再检查一遍:股票必须和这个子任务的输入一致、
     * 链接必须是 http(s)、各字段截到列宽。同一只股票的同一个出处靠唯一键只存一次,所以重跑不会重复。
     */
    @Override
    public void onFinished(TaskEntity task, List<SubtaskEntity> subs) {
        int saved = 0;
        for (SubtaskEntity sub : subs) {
            if (!SubtaskStatus.COMPLETED.name().equals(sub.getStatus()) || sub.getOutput() == null) {
                continue;
            }
            try {
                JsonNode in = objectMapper.readTree(sub.getInput());
                JsonNode out = objectMapper.readTree(sub.getOutput());
                String ticker = in.path("ticker").asText();
                if (ticker.isEmpty() || !ticker.equals(out.path("ticker").asText())) {
                    log.warn("简报输出和输入的股票对不上,丢弃 subtaskId={}", sub.getId());
                    continue;
                }
                LocalDate briefDate = LocalDate.parse(in.path("date").asText());
                for (JsonNode ev : out.path("events")) {
                    saved += save(ticker, briefDate, ev);
                }
            } catch (Exception e) {
                log.warn("简报结果解析失败,跳过 subtaskId={}", sub.getId(), e);
            }
        }
        log.info("简报落库 taskId={} 新增事件={}", task.getId(), saved);
    }

    private int save(String ticker, LocalDate briefDate, JsonNode ev) {
        String url = ev.path("url").asText();
        String title = ev.path("title").asText().strip();
        String summary = ev.path("summary").asText().strip();
        boolean http = url.startsWith("https://") || url.startsWith("http://");
        if (!http || url.length() > 1024 || title.isEmpty() || summary.isEmpty()) {
            return 0;
        }
        StockEventEntity e = new StockEventEntity();
        e.setTicker(ticker);
        e.setBriefDate(briefDate);
        e.setEventDate(dateOrNull(ev.path("eventDate").asText()));
        e.setTitle(cut(title, 200));
        e.setSummary(cut(summary, 1000));
        e.setImportance(Math.max(1, Math.min(3, ev.path("importance").asInt(2))));
        e.setUrl(url);
        e.setUrlHash(sha256(url));
        e.setSourceTitle(cut(ev.path("sourceTitle").asText(null), 300));
        e.setQuote(cut(ev.path("quote").asText(null), 1000));
        e.setForm(cut(ev.path("form").asText(null), 16));
        try {
            return stockEventMapper.insertIgnore(e);
        } catch (RuntimeException ex) {
            log.warn("事件保存失败,跳过 ticker={} url={}", ticker, url, ex);
            return 0;
        }
    }

    private static LocalDate dateOrNull(String s) {
        try {
            return LocalDate.parse(s);
        } catch (DateTimeParseException e) {
            return null;
        }
    }

    private static String cut(String s, int max) {
        return s == null || s.length() <= max ? s : s.substring(0, max);
    }

    private static String sha256(String s) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(s.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
