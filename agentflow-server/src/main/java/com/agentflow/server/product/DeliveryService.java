package com.agentflow.server.product;

import com.agentflow.server.entity.StockEventEntity;
import com.agentflow.server.entity.UserEntity;
import com.agentflow.server.entity.WatchlistEntity;
import com.agentflow.server.mapper.PushLogMapper;
import com.agentflow.server.mapper.StockEventMapper;
import com.agentflow.server.mapper.UserMapper;
import com.agentflow.server.mapper.WatchlistMapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

/**
 * 把简报推到用户自己的 Slack / 飞书。每小时跑一次:轮到谁(他的本地时间到了他设的小时)就把他自选股的新事件合成一条消息。
 *
 * <p>"每人每件事只推一次"靠 push_log 的唯一键:先插入一行占位(抢到才发),发送失败再删掉,下次重试。
 * 所以同一小时触发两次、或者多台 server 同时触发,都不会重复推。
 * 故意不加 @Transactional:占位要立刻提交,别的实例才看得见。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DeliveryService {

    private static final int MAX_IN_MESSAGE = 10;

    private final UserMapper userMapper;
    private final WatchlistMapper watchlistMapper;
    private final StockEventMapper stockEventMapper;
    private final PushLogMapper pushLogMapper;
    private final WebhookClient webhookClient;

    @Value("${agentflow.public-base-url:${PUBLIC_BASE_URL:http://localhost:8080}}")
    private String baseUrl;

    /** @return 发出去的消息数 */
    public int deliverDue(Instant now) {
        int sent = 0;
        // ponytail: 每小时把所有填了推送地址的用户过一遍,一个一个发。几万用户以内够用;
        // 再多就存一列"UTC 推送小时"按索引取,并把发送改成并发。
        for (UserEntity u : userMapper.selectList(new QueryWrapper<UserEntity>().isNotNull("webhook_url"))) {
            try {
                if (isDue(u, now) && deliver(u)) {
                    sent++;
                }
            } catch (RuntimeException e) {
                log.warn("推送出错,跳过这个用户 userId={}", u.getId(), e);   // 不打印 webhook 地址:它等同于密码
            }
        }
        return sent;
    }

    private static boolean isDue(UserEntity u, Instant now) {
        try {
            return now.atZone(ZoneId.of(u.getTimezone())).getHour() == u.getPushHour();
        } catch (DateTimeException | NullPointerException e) {
            return false;
        }
    }

    private boolean deliver(UserEntity u) {
        List<String> tickers = watchlistMapper.selectList(new QueryWrapper<WatchlistEntity>().eq("user_id", u.getId()))
                .stream().map(WatchlistEntity::getTicker).toList();
        if (tickers.isEmpty()) {
            return false;
        }
        // 只看最近 36 小时:一天推一次,多留半天余量给"昨天没发出去"的重试
        List<StockEventEntity> fresh = new ArrayList<>();
        for (StockEventEntity e : stockEventMapper.selectList(new QueryWrapper<StockEventEntity>()
                .in("ticker", tickers).apply("created_at >= NOW() - INTERVAL 36 HOUR")
                .orderByDesc("importance", "id").last("LIMIT 50"))) {
            if (pushLogMapper.claim(u.getId(), e.getId()) == 1) {
                fresh.add(e);
            }
        }
        if (fresh.isEmpty()) {
            return false;
        }
        try {
            webhookClient.send(u.getWebhookUrl(), message(fresh));
            return true;
        } catch (RuntimeException e) {
            // ponytail: 进程恰好在"占位之后、发送之前"被杀,这几条就不会再推(网页上仍然看得到)。
            // 要补上就给 push_log 加状态列 + 超时回收。
            fresh.forEach(ev -> pushLogMapper.release(u.getId(), ev.getId()));
            log.warn("推送失败,下次重试 userId={} events={} cause={}", u.getId(), fresh.size(), e.getMessage());
            return false;
        }
    }

    private String message(List<StockEventEntity> events) {
        StringBuilder sb = new StringBuilder("今日简报 · ").append(events.size()).append(" 条新动态\n");
        for (StockEventEntity e : events.subList(0, Math.min(MAX_IN_MESSAGE, events.size()))) {
            String summary = e.getSummary().length() <= 200 ? e.getSummary() : e.getSummary().substring(0, 200) + "…";
            sb.append("\n【").append(e.getTicker()).append(e.getImportance() >= 3 ? " · 重要" : "").append("】")
                    .append(e.getTitle()).append('\n').append(summary).append('\n').append(e.getUrl()).append('\n');
        }
        if (events.size() > MAX_IN_MESSAGE) {
            sb.append("\n还有 ").append(events.size() - MAX_IN_MESSAGE).append(" 条,见网页");
        }
        return sb.append("\n全部内容: ").append(baseUrl).append("/app.html\n仅为信息整理,不构成投资建议。").toString();
    }
}
