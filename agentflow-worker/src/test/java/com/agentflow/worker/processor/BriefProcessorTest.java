package com.agentflow.worker.processor;

import com.agentflow.common.llm.ChatMessage;
import com.agentflow.common.llm.ChatRequest;
import com.agentflow.common.llm.ChatResponse;
import com.agentflow.common.llm.TokenUsage;
import com.agentflow.worker.agent.Prompts;
import com.agentflow.worker.agent.tool.WebSearchTool;
import com.agentflow.worker.brief.SecFilings;
import com.agentflow.worker.llm.LlmGateway;
import com.agentflow.worker.llm.LlmModelProperties;
import com.agentflow.worker.llm.MockLlmClient;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.time.LocalDate;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

class BriefProcessorTest {

    private static final ObjectMapper OM = new ObjectMapper();
    private static final String INPUT = "{\"ticker\":\"AAPL\",\"name\":\"Apple Inc.\",\"date\":\"2026-11-09\"}";
    private static final WebSearchTool.Hit HIT1 = new WebSearchTool.Hit("Apple beats on revenue",
            "https://news.example/apple-q4", "Apple posted revenue of $94.9 billion, up 6% from a year ago.");
    private static final WebSearchTool.Hit HIT2 = new WebSearchTool.Hit("Apple names new CFO",
            "https://news.example/apple-cfo", "Kevan Parekh will succeed Luca Maestri as chief financial officer.");

    private final LlmGateway gateway = mock(LlmGateway.class);
    private final WebSearchTool search = mock(WebSearchTool.class);
    private final SecFilings sec = mock(SecFilings.class);

    @BeforeEach
    void hits() throws Exception {
        when(search.search(anyString(), anyString())).thenReturn(List.of(HIT1, HIT2));
    }

    private BriefProcessor processor() {
        LlmModelProperties props = new LlmModelProperties();
        LlmModelProperties.Model m = new LlmModelProperties.Model();
        m.setName("m");
        props.setModels(List.of(m));
        return new BriefProcessor(gateway, props, search, sec);
    }

    private JsonNode run(String input, String llmOutput) throws Exception {
        when(gateway.chat(any())).thenReturn(new ChatResponse(llmOutput, new TokenUsage(5, 5), "m"));
        return OM.readTree(processor().process(input));
    }

    @Test
    void turnsSearchHitsIntoEventsThatPointAtTheirSource() throws Exception {
        JsonNode out = run("{\"ticker\":\"AAPL\",\"name\":\"Apple Inc.\",\"date\":\"2026-11-09\","
                        + "\"knownEvents\":[\"苹果发布新款 MacBook\"]}",
                "```json\n{\"events\":["
                        + "{\"title\":\"苹果四季度营收 949 亿美元\",\"summary\":\"营收同比增长 6%。\",\"importance\":3,"
                        + "\"source\":1,\"quote\":\"revenue of $94.9 billion\"},"
                        + "{\"title\":\"苹果任命新 CFO\",\"summary\":\"Kevan Parekh 接任。\",\"importance\":9,\"source\":2}]}\n```");

        assertEquals("AAPL", out.get("ticker").asText());
        assertEquals("2026-11-09", out.get("date").asText());
        JsonNode e1 = out.get("events").get(0);
        assertEquals("苹果四季度营收 949 亿美元", e1.get("title").asText());
        assertEquals(HIT1.url(), e1.get("url").asText());
        assertEquals(HIT1.title(), e1.get("sourceTitle").asText());
        assertEquals("revenue of $94.9 billion", e1.get("quote").asText());
        assertEquals(3, out.get("events").get(1).get("importance").asInt(), "重要度限制在 1~3");
        assertEquals(1, out.at("/usage/calls").asInt());
        assertEquals(0, out.get("warnings").size());
        verify(search).search("Apple Inc. AAPL stock news", "day");

        ArgumentCaptor<ChatRequest> req = ArgumentCaptor.forClass(ChatRequest.class);
        verify(gateway).chat(req.capture());
        List<ChatMessage> msgs = req.getValue().getMessages();
        assertTrue(msgs.get(0).getContent().startsWith(Prompts.BRIEFER_TAG));
        assertTrue(msgs.get(0).getContent().contains("不要给出买入"), "提示词必须禁止投资建议");
        String user = msgs.get(1).getContent();
        assertTrue(user.contains("[1] Apple beats on revenue") && user.contains("[2] Apple names new CFO"), user);
        assertTrue(user.contains("苹果发布新款 MacBook"), "已报道的事件要告诉模型,避免重复:" + user);
    }

    @Test
    void dropsEventsWithoutAValidSource() throws Exception {
        // 网页内容可能诱导模型编造事件或塞进别的链接:事件必须指向我们给它的某一条,链接只用我们自己的
        JsonNode out = run(INPUT, "{\"events\":["
                + "{\"title\":\"编造 1\",\"summary\":\"x\",\"source\":0},"
                + "{\"title\":\"编造 2\",\"summary\":\"x\",\"source\":7},"
                + "{\"title\":\"编造 3\",\"summary\":\"x\",\"source\":\"https://evil.example\"},"
                + "{\"title\":\"编造 4\",\"summary\":\"x\"},"
                + "{\"title\":\"\",\"summary\":\"没有标题\",\"source\":2},"
                + "{\"title\":\"真事件\",\"summary\":\"x\",\"source\":1,\"url\":\"https://evil.example/phish\"},"
                + "{\"title\":\"同一条来源的第二个事件\",\"summary\":\"x\",\"source\":1}]}");
        assertEquals(1, out.get("events").size(), out.toString());
        assertEquals("真事件", out.get("events").get(0).get("title").asText());
        assertEquals(HIT1.url(), out.get("events").get(0).get("url").asText());
        assertFalse(out.toString().contains("evil.example"));
    }

    @Test
    void dropsQuotesThatAreNotInTheSource() throws Exception {
        JsonNode out = run(INPUT, "{\"events\":["
                + "{\"title\":\"a\",\"summary\":\"x\",\"source\":1,\"quote\":\"revenue of $120 billion\"},"
                + "{\"title\":\"b\",\"summary\":\"x\",\"source\":2,\"quote\":\"kevan parekh  will succeed Luca Maestri\"}]}");
        assertEquals(2, out.get("events").size());
        assertFalse(out.get("events").get(0).has("quote"), "原文里没有的引文不能当原文展示");
        assertTrue(out.get("events").get(1).has("quote"), "只差大小写和空格的算逐字");
    }

    @Test
    void alreadySeenUrlsAreSkippedAndNothingNewMeansNoModelCall() throws Exception {
        String input = "{\"ticker\":\"AAPL\",\"name\":\"Apple Inc.\",\"date\":\"2026-11-09\",\"seenUrls\":[\""
                + HIT1.url() + "\",\"" + HIT2.url() + "\"]}";
        JsonNode out = OM.readTree(processor().process(input));
        assertEquals(0, out.get("events").size());
        assertEquals(0, out.at("/usage/calls").asInt());
        verifyNoInteractions(gateway);
    }

    @Test
    void whenEverySourceFailsTheSubtaskFailsSoItIsRetried() throws Exception {
        when(search.search(anyString(), anyString())).thenThrow(new IllegalStateException("Tavily HTTP 500"));
        assertThrows(IllegalStateException.class, () -> processor().process(INPUT));
        verifyNoInteractions(gateway);
    }

    @Test
    void oneFailedSourceBecomesAWarningWhenTheOtherWorks() throws Exception {
        when(search.search(anyString(), anyString())).thenThrow(new IllegalStateException("Tavily HTTP 500"));
        when(sec.enabled()).thenReturn(true);
        when(sec.recent("0000320193", LocalDate.parse("2026-11-06"))).thenReturn(List.of(new SecFilings.Filing(
                "8-K", "2026-11-06", "8-K (Items 2.02, 9.01)", "https://www.sec.gov/Archives/edgar/data/320193/x/a.htm",
                "Apple reports fourth quarter results. Revenue of $94.9 billion.")));
        JsonNode out = run("{\"ticker\":\"AAPL\",\"cik\":\"0000320193\",\"name\":\"Apple Inc.\",\"date\":\"2026-11-09\"}",
                "{\"events\":[{\"title\":\"苹果公布四季度业绩\",\"summary\":\"营收 949 亿美元。\",\"importance\":3,\"source\":1,"
                        + "\"quote\":\"Revenue of $94.9 billion\"}]}");
        JsonNode e = out.get("events").get(0);
        assertEquals("8-K", e.get("form").asText());
        assertEquals("2026-11-06", e.get("eventDate").asText());
        assertTrue(e.get("url").asText().startsWith("https://www.sec.gov/"));
        assertEquals(1, out.get("warnings").size());
        assertTrue(out.get("warnings").get(0).asText().contains("新闻"), out.get("warnings").toString());
    }

    @Test
    void secIsSkippedWithoutACikOrWhenDisabled() throws Exception {
        when(sec.enabled()).thenReturn(false);
        run("{\"ticker\":\"AAPL\",\"cik\":\"0000320193\",\"name\":\"Apple Inc.\",\"date\":\"2026-11-09\"}", "{\"events\":[]}");
        when(sec.enabled()).thenReturn(true);
        run(INPUT, "{\"events\":[]}");
        verify(sec, never()).recent(anyString(), any());
    }

    @Test
    void mockModelOutputSurvivesTheChecks() throws Exception {
        // mock 模式(默认配置、演示环境)也要走完整条链路:它返回的引文必须能通过逐字校验
        MockLlmClient mockModel = new MockLlmClient();
        when(gateway.chat(any())).thenAnswer(inv -> {
            ChatRequest r = inv.getArgument(0);
            r.setModel("m");
            return mockModel.chat(r);
        });
        JsonNode out = OM.readTree(processor().process(INPUT));
        assertEquals(1, out.get("events").size(), out.toString());
        assertEquals(HIT1.url(), out.get("events").get(0).get("url").asText());
        assertTrue(out.get("events").get(0).has("quote"));
    }

    @Test
    void rejectsInputWithoutTickerOrDate() {
        assertThrows(IllegalArgumentException.class, () -> processor().process("{\"date\":\"2026-11-09\"}"));
        assertThrows(IllegalArgumentException.class, () -> processor().process("{\"ticker\":\"AAPL\"}"));
        assertThrows(IllegalArgumentException.class, () -> processor().process("{\"ticker\":\"AAPL\",\"date\":\"昨天\"}"));
    }
}
