package com.agentflow.server.service;

import com.agentflow.common.mq.ResultMessage;
import com.agentflow.server.coordinator.DailyBriefDecomposer;
import com.agentflow.server.coordinator.SubtaskDef;
import com.agentflow.server.entity.StockEventEntity;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.entity.WatchlistEntity;
import com.agentflow.server.mapper.StockEventMapper;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import com.agentflow.server.mapper.WatchlistMapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * DAILY_BRIEF:按自选股拆成每只股票一个 BRIEF 子任务;全部落定后把事件写进 stock_event。
 * 用不存在的股票代码(ZQX*)并在 payload 里点名,不受本机库里其他数据影响。
 */
@SpringBootTest(properties = "agentflow.sec.user-agent=")
@Transactional
class DailyBriefFlowTest {

    private static final ObjectMapper OM = new ObjectMapper();
    private static final String DAY = "2026-11-09";

    @Autowired private DailyBriefDecomposer decomposer;
    @Autowired private ResultHandleService results;
    @Autowired private TaskMapper taskMapper;
    @Autowired private SubtaskMapper subtaskMapper;
    @Autowired private WatchlistMapper watchlistMapper;
    @Autowired private StockEventMapper stockEventMapper;
    @MockBean private KafkaTemplate<String, Object> kafkaTemplate;

    private void watch(long userId, String ticker, String name) {
        WatchlistEntity w = new WatchlistEntity();
        w.setUserId(userId);
        w.setTicker(ticker);
        w.setCompanyName(name);
        watchlistMapper.insert(w);
    }

    private List<StockEventEntity> events(String ticker) {
        return stockEventMapper.selectList(new QueryWrapper<StockEventEntity>().eq("ticker", ticker).orderByAsc("id"));
    }

    private JsonNode payload(String json) throws Exception {
        return OM.readTree(json);
    }

    @Test
    void oneSubtaskPerWatchedTickerCarryingWhatWasAlreadyReported() throws Exception {
        watch(9_000_001L, "ZQXA", "Alpha Corp");
        watch(9_000_002L, "ZQXA", "Alpha Corp");
        watch(9_000_002L, "ZQXB", "Beta Inc");
        StockEventEntity old = new StockEventEntity();
        old.setTicker("ZQXA");
        old.setBriefDate(LocalDate.parse("2026-11-08"));
        old.setTitle("Alpha 发布三季报");
        old.setSummary("摘要");
        old.setImportance(3);
        old.setUrl("https://news.example/alpha-q3");
        old.setUrlHash("a".repeat(64));
        stockEventMapper.insertIgnore(old);

        List<SubtaskDef> defs = decomposer.decompose(payload(
                "{\"tickers\":[\"ZQXB\",\"ZQXA\",\"ZQXC\"],\"date\":\"" + DAY + "\"}"));

        assertEquals(2, defs.size(), "没人关注的 ZQXC 不生成");
        assertTrue(defs.stream().allMatch(d -> "BRIEF".equals(d.kind())));
        assertEquals(List.of(0, 1), defs.stream().map(SubtaskDef::seq).toList());
        JsonNode a = OM.readTree(defs.get(0).inputJson());
        assertEquals("ZQXA", a.get("ticker").asText(), "关注的人多的排前面");
        assertEquals("Alpha Corp", a.get("name").asText());
        assertEquals(DAY, a.get("date").asText());
        assertEquals("https://news.example/alpha-q3", a.get("seenUrls").get(0).asText());
        assertEquals("Alpha 发布三季报", a.get("knownEvents").get(0).asText());
        assertFalse(a.has("cik"), "没配 SEC User-Agent 时查不到 CIK,worker 只用新闻");
        JsonNode b = OM.readTree(defs.get(1).inputJson());
        assertEquals("ZQXB", b.get("ticker").asText());
        assertEquals(0, b.get("seenUrls").size());
    }

    @Test
    void rejectsBadPayloadsAndAnEmptyWatchlist() throws Exception {
        watch(9_000_001L, "ZQXA", "Alpha Corp");
        assertThrows(IllegalArgumentException.class, () -> decomposer.decompose(payload("{\"tickers\":[\"ZQXC\"]}")),
                "点名的股票没人关注");
        assertThrows(IllegalArgumentException.class,
                () -> decomposer.decompose(payload("{\"tickers\":[\"ZQXA\"],\"date\":\"昨天\"}")));
        assertThrows(IllegalArgumentException.class,
                () -> decomposer.decompose(payload("{\"tickers\":[\"ZQXA' OR '1'='1\"]}")));
        assertThrows(IllegalArgumentException.class, () -> decomposer.decompose(payload("{\"tickers\":\"ZQXA\"}")));
        assertEquals(1, decomposer.decompose(payload("{\"tickers\":[\"ZQXA\"]}")).size(), "date 可省略:默认美东今天");
    }

    /** RUNNING 的 DAILY_BRIEF 任务 + 两个已分发的 BRIEF 子任务(ZQXA、ZQXB)。 */
    private long[] runningTask() {
        TaskEntity t = new TaskEntity();
        t.setTaskUuid(UUID.randomUUID().toString());
        t.setType("DAILY_BRIEF");
        t.setStatus("RUNNING");
        t.setPayload("{}");
        t.setSubtaskTotal(2);
        t.setSubtaskDone(0);
        t.setSubtaskFailed(0);
        taskMapper.insert(t);
        long[] ids = new long[3];
        ids[0] = t.getId();
        String[] tickers = {"ZQXA", "ZQXB"};
        for (int i = 0; i < 2; i++) {
            SubtaskEntity s = new SubtaskEntity();
            s.setSubtaskUuid(UUID.randomUUID().toString());
            s.setTaskId(t.getId());
            s.setSeq(i);
            s.setKind("BRIEF");
            s.setStatus("DISPATCHED");
            s.setInput("{\"ticker\":\"" + tickers[i] + "\",\"name\":\"x\",\"date\":\"" + DAY + "\"}");
            subtaskMapper.insert(s);
            ids[i + 1] = s.getId();
        }
        return ids;
    }

    @Test
    void finishedBriefSavesItsEventsAndNothingThatDoesNotBelong() {
        long[] ids = runningTask();
        String longTitle = "长".repeat(400);
        results.handle(new ResultMessage(ids[0], ids[1], true, "{\"ticker\":\"ZQXA\",\"date\":\"" + DAY + "\",\"events\":["
                + "{\"title\":\"Alpha 收购 Gamma\",\"summary\":\"作价 12 亿美元。\",\"importance\":9,"
                + "\"url\":\"https://www.sec.gov/Archives/edgar/data/1/2/a.htm\",\"sourceTitle\":\"8-K (Items 1.01)\","
                + "\"quote\":\"for $1.2 billion\",\"form\":\"8-K\",\"eventDate\":\"2026-11-06\"},"
                + "{\"title\":\"" + longTitle + "\",\"summary\":\"标题超长也要存得下\",\"importance\":1,"
                + "\"url\":\"https://news.example/long\",\"eventDate\":\"不是日期\"},"
                + "{\"title\":\"坏链接\",\"summary\":\"x\",\"importance\":2,\"url\":\"javascript:alert(1)\"},"
                + "{\"title\":\"\",\"summary\":\"没有标题\",\"importance\":2,\"url\":\"https://news.example/no-title\"},"
                + "{\"title\":\"没有摘要\",\"importance\":2,\"url\":\"https://news.example/no-summary\"}"
                + "],\"warnings\":[],\"usage\":{}}", null));
        assertTrue(events("ZQXA").isEmpty(), "任务还没落定,先不写");

        // 第二个子任务的输出说自己是别的股票:和它的输入对不上,整份不要
        results.handle(new ResultMessage(ids[0], ids[2], true, "{\"ticker\":\"ZQXA\",\"date\":\"" + DAY + "\",\"events\":["
                + "{\"title\":\"串号\",\"summary\":\"x\",\"importance\":2,\"url\":\"https://news.example/wrong\"}]}", null));

        assertEquals("COMPLETED", taskMapper.selectById(ids[0]).getStatus());
        List<StockEventEntity> saved = events("ZQXA");
        assertEquals(2, saved.size(), saved.toString());
        StockEventEntity e = saved.get(0);
        assertEquals("Alpha 收购 Gamma", e.getTitle());
        assertEquals(3, e.getImportance(), "重要度限制在 1~3");
        assertEquals(LocalDate.parse(DAY), e.getBriefDate());
        assertEquals(LocalDate.parse("2026-11-06"), e.getEventDate());
        assertEquals("8-K", e.getForm());
        assertEquals("for $1.2 billion", e.getQuote());
        assertEquals(64, e.getUrlHash().length());
        assertTrue(saved.get(1).getTitle().length() <= 200);
        assertNull(saved.get(1).getEventDate());
        assertTrue(events("ZQXB").isEmpty());

        // 同一批结果再落一次(任务重跑):唯一键兜住,不出现重复事件
        decomposer.onFinished(taskMapper.selectById(ids[0]), subtaskMapper.selectList(
                new QueryWrapper<SubtaskEntity>().eq("task_id", ids[0]).orderByAsc("seq")));
        assertEquals(2, events("ZQXA").size());
    }

    @Test
    void aFailedTickerDoesNotBlockTheOthers() {
        long[] ids = runningTask();
        results.handle(new ResultMessage(ids[0], ids[1], false, null, "搜索挂了"));
        results.handle(new ResultMessage(ids[0], ids[2], true, "{\"ticker\":\"ZQXB\",\"date\":\"" + DAY + "\",\"events\":["
                + "{\"title\":\"Beta 回购\",\"summary\":\"回购 5 亿美元。\",\"importance\":2,\"url\":\"https://news.example/beta\"}]}",
                null));
        assertEquals("PARTIAL_FAILED", taskMapper.selectById(ids[0]).getStatus());
        assertEquals(1, events("ZQXB").size());
    }


    @Test
    void tickersPastTheCapTakeTurnsAcrossDays() throws Exception {
        // 关注人数相同的股票超过上限时,不能永远是字母靠前的那只:按日期轮换,谁都轮得到
        watch(9_000_001L, "ZQXA", "Alpha Corp");
        watch(9_000_002L, "ZQXB", "Beta Inc");
        ReflectionTestUtils.setField(decomposer, "maxTickers", 1);
        try {
            Set<String> picked = new HashSet<>();
            for (int d = 1; d <= 30; d++) {
                List<SubtaskDef> defs = decomposer.decompose(payload(
                        "{\"tickers\":[\"ZQXA\",\"ZQXB\"],\"date\":\"" + LocalDate.of(2026, 11, d) + "\"}"));
                assertEquals(1, defs.size());
                picked.add(OM.readTree(defs.get(0).inputJson()).get("ticker").asText());
            }
            assertEquals(Set.of("ZQXA", "ZQXB"), picked);
        } finally {
            ReflectionTestUtils.setField(decomposer, "maxTickers", 100);
        }
    }
}
