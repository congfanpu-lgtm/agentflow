package com.agentflow.server.product;

import com.agentflow.server.entity.StockEventEntity;
import com.agentflow.server.entity.UserEntity;
import com.agentflow.server.entity.WatchlistEntity;
import com.agentflow.server.mapper.StockEventMapper;
import com.agentflow.server.mapper.UserMapper;
import com.agentflow.server.mapper.WatchlistMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;
import java.time.LocalDate;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

/**
 * 到了用户设定的小时,把他自选股的新事件合成一条消息推出去;每人每件事只推一次。
 * 每个用户用一个独一无二的 webhook 地址,断言只看发往这个地址的消息,不受本机库里其他用户影响。
 */
@SpringBootTest(properties = "agentflow.public-base-url=https://brief.example")
@Transactional
class DeliveryServiceTest {

    /** 北京时间 08:05 —— 默认推送小时(8 点,Asia/Shanghai)的用户此刻该收到。 */
    private static final Instant EIGHT_IN_SHANGHAI = Instant.parse("2026-11-10T00:05:00Z");

    @Autowired private DeliveryService delivery;
    @Autowired private UserMapper userMapper;
    @Autowired private WatchlistMapper watchlistMapper;
    @Autowired private StockEventMapper stockEventMapper;
    @MockBean private WebhookClient webhook;
    @MockBean private KafkaTemplate<String, Object> kafkaTemplate;

    private int seq;

    /** 新用户关注 tickers,返回他的 webhook 地址。 */
    private String user(int pushHour, String timezone, boolean withWebhook, String... tickers) {
        String hook = "https://hooks.slack.com/services/T/B/" + UUID.randomUUID();
        UserEntity u = new UserEntity();
        u.setEmail(UUID.randomUUID() + "@test.example");
        u.setPasswordHash("x");
        u.setWebhookUrl(withWebhook ? hook : null);
        u.setPushHour(pushHour);
        u.setTimezone(timezone);
        userMapper.insert(u);
        for (String t : tickers) {
            WatchlistEntity w = new WatchlistEntity();
            w.setUserId(u.getId());
            w.setTicker(t);
            w.setCompanyName(t + " Corp");
            watchlistMapper.insert(w);
        }
        return hook;
    }

    private void event(String ticker, String title, int importance) {
        StockEventEntity e = new StockEventEntity();
        e.setTicker(ticker);
        e.setBriefDate(LocalDate.parse("2026-11-09"));
        e.setTitle(title);
        e.setSummary(title + "的摘要");
        e.setImportance(importance);
        e.setUrl("https://news.example/" + (++seq));
        e.setUrlHash(UUID.randomUUID().toString().replace("-", "").repeat(2));
        assertEquals(1, stockEventMapper.insertIgnore(e));
    }

    private String lastMessageTo(String hook, int expectedTotal) {
        ArgumentCaptor<String> text = ArgumentCaptor.forClass(String.class);
        verify(webhook, times(expectedTotal)).send(eq(hook), text.capture());
        return text.getValue();
    }

    @Test
    void eachEventIsPushedOncePerUser() {
        String a = user(8, "Asia/Shanghai", true, "ZQXA");
        String b = user(8, "Asia/Shanghai", true, "ZQXA", "ZQXB");
        event("ZQXA", "Alpha 发布财报", 3);
        event("ZQXB", "Beta 换 CEO", 2);

        delivery.deliverDue(EIGHT_IN_SHANGHAI);
        String toA = lastMessageTo(a, 1);
        assertTrue(toA.contains("Alpha 发布财报") && toA.contains("Alpha 发布财报的摘要") && toA.contains("ZQXA"), toA);
        assertFalse(toA.contains("Beta"), "没关注的股票不推");
        assertTrue(toA.contains("https://news.example/1"), "带出处链接");
        assertTrue(toA.contains("https://brief.example/app.html"), toA);
        assertTrue(toA.contains("不构成投资建议"), toA);
        String toB = lastMessageTo(b, 1);
        assertTrue(toB.contains("Alpha 发布财报") && toB.contains("Beta 换 CEO"), toB);
        assertTrue(toB.indexOf("Alpha 发布财报") < toB.indexOf("Beta 换 CEO"), "重要的排前面");

        delivery.deliverDue(EIGHT_IN_SHANGHAI);   // 同一小时又触发一次(重启、多实例)
        verify(webhook, times(1)).send(eq(a), any());
        verify(webhook, times(1)).send(eq(b), any());

        event("ZQXA", "Alpha 宣布回购", 2);
        delivery.deliverDue(EIGHT_IN_SHANGHAI.plusSeconds(86_400));
        String next = lastMessageTo(a, 2);
        assertTrue(next.contains("Alpha 宣布回购"), next);
        assertFalse(next.contains("Alpha 发布财报"), "昨天推过的不再推");
    }

    @Test
    void failedPushIsRetriedNextTime() {
        String a = user(8, "Asia/Shanghai", true, "ZQXA");
        String b = user(8, "Asia/Shanghai", true, "ZQXA");
        event("ZQXA", "Alpha 发布财报", 3);
        doThrow(new IllegalStateException("推送失败:HTTP 500")).doNothing().when(webhook).send(eq(a), any());

        delivery.deliverDue(EIGHT_IN_SHANGHAI);   // a 失败不能连累 b,也不能抛出来
        verify(webhook, times(1)).send(eq(b), any());

        delivery.deliverDue(EIGHT_IN_SHANGHAI);
        assertTrue(lastMessageTo(a, 2).contains("Alpha 发布财报"), "上次没发出去的,这次还在");
        verify(webhook, times(1)).send(eq(b), any());
    }

    @Test
    void nothingIsSentOutsideThePushHourOrWithoutNews() {
        String nineOClock = user(9, "Asia/Shanghai", true, "ZQXA");
        String newYorker = user(8, "America/New_York", true, "ZQXA");   // 纽约此刻是晚上 7 点
        String nothingNew = user(8, "Asia/Shanghai", true, "ZQXC");
        user(8, "Asia/Shanghai", false, "ZQXA");                        // 没填推送地址:只在网页看
        event("ZQXA", "Alpha 发布财报", 3);

        delivery.deliverDue(EIGHT_IN_SHANGHAI);
        verify(webhook, never()).send(eq(nineOClock), any());
        verify(webhook, never()).send(eq(newYorker), any());
        verify(webhook, never()).send(eq(nothingNew), any());

        delivery.deliverDue(Instant.parse("2026-11-10T13:05:00Z"));     // 纽约 08:05
        assertTrue(lastMessageTo(newYorker, 1).contains("Alpha 发布财报"));
    }

    @Test
    void longListsAreCutToTenWithAPointerToThePage() {
        String a = user(8, "Asia/Shanghai", true, "ZQXA");
        for (int i = 1; i <= 12; i++) {
            event("ZQXA", "事件" + i + "号", i <= 10 ? 2 : 1);
        }
        delivery.deliverDue(EIGHT_IN_SHANGHAI);
        String text = lastMessageTo(a, 1);
        assertTrue(text.contains("事件10号") && !text.contains("事件11号") && !text.contains("事件12号"), text);
        assertTrue(text.contains("还有 2 条"), text);

        delivery.deliverDue(EIGHT_IN_SHANGHAI);
        verify(webhook, times(1)).send(eq(a), any());   // 没放进消息的那 2 条也算推过,不会明天再冒出来
    }
}
