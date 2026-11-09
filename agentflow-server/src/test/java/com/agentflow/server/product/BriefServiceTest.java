package com.agentflow.server.product;

import com.agentflow.server.service.TaskSubmitService;
import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/** 触发简报任务的两条路:定时全量、用户新加股票时单只补一份。去重键在 Redis 里,所以每个用例用一个随机的"未来日期"。 */
@SpringBootTest(properties = "agentflow.brief.max-on-demand=2")
class BriefServiceTest {

    @Autowired private BriefService service;
    @MockBean private TaskSubmitService submit;
    @MockBean private KafkaTemplate<String, Object> kafkaTemplate;

    private final LocalDate day = LocalDate.of(ThreadLocalRandom.current().nextInt(3000, 9000),
            ThreadLocalRandom.current().nextInt(1, 13), ThreadLocalRandom.current().nextInt(1, 28));

    @Test
    void aTickerGetsOneOnDemandBriefPerDay() {
        assertTrue(service.triggerForTicker("ZQXA", day));
        assertFalse(service.triggerForTicker("ZQXA", day), "同一天第二个人加同一只股票,不重复生成");
        verify(submit, times(1)).submit(eq("DAILY_BRIEF"),
                argThat((JsonNode p) -> p.path("tickers").size() == 1 && "ZQXA".equals(p.path("tickers").get(0).asText())));
    }

    @Test
    void onDemandBriefsHaveADailyCap() {
        assertTrue(service.triggerForTicker("ZQXA", day));
        assertTrue(service.triggerForTicker("ZQXB", day));
        assertFalse(service.triggerForTicker("ZQXC", day), "超过每天上限就不再临时生成,等下一次定时任务");
        verify(submit, times(2)).submit(any(), any());
    }

    @Test
    void theDailyRunHappensOncePerSlotAndAnEmptyWatchlistIsNotAnError() {
        ZonedDateTime slot = day.atTime(7, 0).atZone(BriefService.MARKET_ZONE);
        when(submit.submit(any(), any())).thenThrow(new IllegalArgumentException("没有需要生成简报的股票")).thenReturn(null);
        assertFalse(service.triggerDaily(slot), "没人关注任何股票:什么都不做");
        assertTrue(service.triggerDaily(slot), "上一次没生成,这次还能再试");
        assertFalse(service.triggerDaily(slot.plusMinutes(20)), "同一个整点只跑一次(多实例部署时每台机器都会触发)");
        assertTrue(service.triggerDaily(slot.plusHours(11)));
        verify(submit, times(3)).submit(eq("DAILY_BRIEF"), any());
    }
}
