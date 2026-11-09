package com.agentflow.server.product;

import com.agentflow.server.entity.StockEventEntity;
import com.agentflow.server.entity.UserEntity;
import com.agentflow.server.entity.WatchlistEntity;
import com.agentflow.server.mapper.StockEventMapper;
import com.agentflow.server.mapper.UserMapper;
import com.agentflow.server.mapper.WatchlistMapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import jakarta.servlet.http.HttpServletRequest;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.net.URI;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * 登录用户自己的数据。每个方法第一步都是 {@code sessions.require(req)} 取当前用户 id,
 * 之后所有查询都带这个 id —— 接口里没有任何"传一个用户 id 进来"的参数,所以看不到也改不了别人的数据。
 * 路径段只用小写字母和连字符:DemoGuard 靠这个形状识别产品接口(见其注释)。
 */
@Slf4j
@RestController
@RequestMapping("/api/v1/me")
@RequiredArgsConstructor
public class MeController {

    private static final Pattern TICKER = Pattern.compile("^[A-Z]{1,5}(-[A-Z]{1,2})?$");
    private static final int MAX_TICKERS = 20;

    private final Sessions sessions;
    private final Throttle throttle;
    private final UserMapper userMapper;
    private final WatchlistMapper watchlistMapper;
    private final StockEventMapper stockEventMapper;
    private final TickerDirectory tickerDirectory;
    private final WebhookClient webhookClient;
    private final BriefService briefService;

    public record TickerBody(String ticker) {
    }

    /** 三项都可以不传:不传 = 不改。webhookUrl 传空串 = 清除。 */
    public record SettingsBody(String webhookUrl, Integer pushHour, String timezone) {
    }

    @GetMapping
    public Map<String, Object> me(HttpServletRequest req) {
        UserEntity user = user(req);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("email", user.getEmail());
        out.put("webhook", mask(user.getWebhookUrl()));
        out.put("pushHour", user.getPushHour());
        out.put("timezone", user.getTimezone());
        return out;
    }

    @GetMapping("/watchlist")
    public List<Map<String, String>> watchlist(HttpServletRequest req) {
        return watchlistOf(sessions.require(req)).stream()
                .map(w -> Map.of("ticker", w.getTicker(), "companyName", w.getCompanyName())).toList();
    }

    @PostMapping("/watchlist")
    public Map<String, String> addTicker(@RequestBody TickerBody body, HttpServletRequest req) {
        long userId = sessions.require(req);
        String ticker = ticker(body.ticker());
        List<WatchlistEntity> mine = watchlistOf(userId);
        for (WatchlistEntity w : mine) {
            if (w.getTicker().equals(ticker)) {   // 已经在列表里:什么都不做
                return Map.of("ticker", ticker, "companyName", w.getCompanyName());
            }
        }
        // ponytail: 先数后插,同一用户并发添加可能多出一两只;要严格就给 app_user 加计数列并在 SQL 里比较
        if (mine.size() >= MAX_TICKERS) {
            throw bad("最多关注 " + MAX_TICKERS + " 只股票");
        }
        TickerDirectory.Company company = tickerDirectory.find(ticker).orElseThrow(() -> bad("找不到这只股票:" + ticker));
        WatchlistEntity w = new WatchlistEntity();
        w.setUserId(userId);
        w.setTicker(ticker);
        w.setCompanyName(truncate(company.name(), 200));
        try {
            watchlistMapper.insert(w);
        } catch (DuplicateKeyException ignored) {
            // 同一请求连点两次:唯一索引兜住,当作已添加
            return Map.of("ticker", ticker, "companyName", w.getCompanyName());
        }
        try {
            briefService.triggerForTicker(ticker);   // 新加的股票马上补一份简报
        } catch (RuntimeException e) {
            log.warn("临时简报发起失败(不影响添加,下一次定时任务会带上) ticker={}", ticker, e);
        }
        return Map.of("ticker", ticker, "companyName", w.getCompanyName());
    }

    @DeleteMapping("/watchlist")
    public Map<String, Boolean> removeTicker(@RequestParam("ticker") String ticker, HttpServletRequest req) {
        long userId = sessions.require(req);
        watchlistMapper.delete(new QueryWrapper<WatchlistEntity>().eq("user_id", userId).eq("ticker", ticker(ticker)));
        return Map.of("ok", true);
    }

    /** 我关注的股票最近 days 天(最多 30)的事件,新的、重要的在前。 */
    @GetMapping("/events")
    public List<Map<String, Object>> events(@RequestParam(name = "days", defaultValue = "7") int days, HttpServletRequest req) {
        List<String> tickers = watchlistOf(sessions.require(req)).stream().map(WatchlistEntity::getTicker).toList();
        if (tickers.isEmpty()) {
            return List.of();
        }
        LocalDate since = LocalDate.now(BriefService.MARKET_ZONE).minusDays(Math.max(1, Math.min(days, 30)));
        List<Map<String, Object>> out = new ArrayList<>();
        for (StockEventEntity e : stockEventMapper.selectList(new QueryWrapper<StockEventEntity>()
                .in("ticker", tickers).ge("brief_date", since)
                .orderByDesc("brief_date", "importance", "id").last("LIMIT 500"))) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("ticker", e.getTicker());
            m.put("date", String.valueOf(e.getBriefDate()));
            m.put("eventDate", e.getEventDate() == null ? null : e.getEventDate().toString());
            m.put("title", e.getTitle());
            m.put("summary", e.getSummary());
            m.put("importance", e.getImportance());
            m.put("url", e.getUrl());
            m.put("sourceTitle", e.getSourceTitle());
            m.put("quote", e.getQuote());
            m.put("form", e.getForm());
            out.add(m);
        }
        return out;
    }

    @PutMapping("/settings")
    public Map<String, Boolean> settings(@RequestBody SettingsBody body, HttpServletRequest req) {
        UserEntity user = user(req);
        String webhook = user.getWebhookUrl();
        if (body.webhookUrl() != null) {
            try {
                webhook = body.webhookUrl().isBlank() ? null : WebhookClient.validate(body.webhookUrl());
            } catch (IllegalArgumentException e) {
                throw bad(e.getMessage());
            }
        }
        int pushHour = body.pushHour() == null ? user.getPushHour() : body.pushHour();
        if (pushHour < 0 || pushHour > 23) {
            throw bad("推送时间需为 0 到 23 点");
        }
        String timezone = body.timezone() == null ? user.getTimezone() : body.timezone();
        try {
            if (timezone.length() > 64) {
                throw new DateTimeException("too long");
            }
            ZoneId.of(timezone);
        } catch (DateTimeException e) {
            throw bad("无法识别的时区");
        }
        userMapper.updateSettings(user.getId(), webhook, pushHour, timezone);
        return Map.of("ok", true);
    }

    /** 往自己保存的推送地址发一条测试消息。每人每小时 5 次,免得被拿来刷别人的群。 */
    @PostMapping("/webhook-test")
    public Map<String, Boolean> webhookTest(HttpServletRequest req) {
        UserEntity user = user(req);
        if (user.getWebhookUrl() == null) {
            throw bad("还没有设置推送地址");
        }
        if (!throttle.allow("webhook-test:" + user.getId(), 5, Duration.ofHours(1))) {
            throw new ResponseStatusException(HttpStatus.TOO_MANY_REQUESTS, "测试太频繁,请一小时后再试");
        }
        try {
            webhookClient.send(user.getWebhookUrl(), "AgentFlow 测试消息:收到这条说明推送地址可用。");
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw new ResponseStatusException(HttpStatus.BAD_GATEWAY, e.getMessage());
        }
        return Map.of("ok", true);
    }

    private UserEntity user(HttpServletRequest req) {
        UserEntity user = userMapper.selectById(sessions.require(req));
        if (user == null) {   // 会话还在但账号没了
            throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "请先登录");
        }
        return user;
    }

    private List<WatchlistEntity> watchlistOf(long userId) {
        return watchlistMapper.selectList(new QueryWrapper<WatchlistEntity>().eq("user_id", userId).orderByAsc("id"));
    }

    /** 转大写,BRK.B → BRK-B(SEC 的写法);格式不对抛 400。 */
    private static String ticker(String raw) {
        String t = raw == null ? "" : raw.trim().toUpperCase(Locale.ROOT).replace('.', '-');
        if (!TICKER.matcher(t).matches()) {
            throw bad("股票代码格式不对,例如 AAPL、BRK.B");
        }
        return t;
    }

    /** 推送地址等于"往那个群发消息的权限",存了就不再完整回显:只给主机名和末 4 位。 */
    private static String mask(String webhookUrl) {
        return webhookUrl == null ? null
                : URI.create(webhookUrl).getHost() + "/…" + webhookUrl.substring(webhookUrl.length() - 4);
    }

    private static String truncate(String s, int max) {
        return s.length() <= max ? s : s.substring(0, max);
    }

    private static ResponseStatusException bad(String message) {
        return new ResponseStatusException(HttpStatus.BAD_REQUEST, message);
    }
}
