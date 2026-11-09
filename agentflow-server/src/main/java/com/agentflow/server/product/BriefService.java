package com.agentflow.server.product;

import com.agentflow.server.policy.SideEffectPolicy;
import com.agentflow.server.service.TaskSubmitService;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * 发起简报任务的两条路:定时把所有自选股跑一遍;用户新加一只股票时单独给它补一份(不用等到下一次定时)。
 * 两条路都用 SideEffectPolicy(Redis 占位)保证"同一件事只发起一次"——多台 server 同时到点、
 * 或者几个人同一天加同一只股票,都只会有一个任务。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class BriefService {

    /** 简报按美股交易日算"今天"。 */
    public static final ZoneId MARKET_ZONE = ZoneId.of("America/New_York");
    private static final String TYPE = "DAILY_BRIEF";

    private final TaskSubmitService submit;
    private final SideEffectPolicy policy;
    private final Throttle throttle;
    private final ObjectMapper objectMapper;

    /** 每天最多临时补多少只:防止有人批量注册、乱加股票来消耗模型额度。超了就等下一次定时任务。 */
    @Value("${agentflow.brief.max-on-demand:300}")
    private int maxOnDemand;

    /** @return 这一次是否真的发起了任务 */
    public boolean triggerDaily() {
        return triggerDaily(ZonedDateTime.now(MARKET_ZONE));
    }

    boolean triggerDaily(ZonedDateTime now) {
        String slot = now.toLocalDate() + "T" + now.getHour();
        try {
            return policy.execute("brief-run:" + slot, () -> submit.submit(TYPE, objectMapper.createObjectNode()));
        } catch (IllegalArgumentException e) {
            log.info("本轮简报没有发起: {}", e.getMessage());   // 还没人关注任何股票;占位已释放,下次照常
            return false;
        }
    }

    public boolean triggerForTicker(String ticker) {
        return triggerForTicker(ticker, LocalDate.now(MARKET_ZONE));
    }

    boolean triggerForTicker(String ticker, LocalDate date) {
        boolean[] submitted = {false};
        try {
            policy.execute("brief-ticker:" + ticker + ":" + date, () -> {
                if (!throttle.allow("brief-on-demand:" + date, maxOnDemand, Duration.ofDays(1))) {
                    log.warn("今天临时简报已到上限 {},{} 等下一次定时任务", maxOnDemand, ticker);
                    return;
                }
                ObjectNode payload = objectMapper.createObjectNode();
                payload.putArray("tickers").add(ticker);
                submit.submit(TYPE, payload);
                submitted[0] = true;
            });
        } catch (IllegalArgumentException e) {
            log.info("{} 的临时简报没有发起: {}", ticker, e.getMessage());
        }
        return submitted[0];
    }
}
