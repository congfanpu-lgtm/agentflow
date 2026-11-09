package com.agentflow.worker.brief;

import com.sun.net.httpserver.HttpServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

/** SEC 公告:本地桩模拟 data.sec.gov 的 submissions 接口和 www.sec.gov 的 Archives 目录。 */
class SecFilingsTest {

    private static final String UA = "agentflow-test admin@example.com";
    private static final String DIR = "/Archives/edgar/data/320193/000032019326000101/";
    // 顺序与 SEC 一致:最新的在前。第 2 条是内部人交易(表单 4),第 4 条太旧。
    private static final String SUBMISSIONS = """
            {"cik":"320193","name":"Apple Inc.","filings":{"recent":{
              "accessionNumber":["0000320193-26-000101","0000320193-26-000100","0000320193-26-000099","0000320193-26-000090"],
              "filingDate":["2026-11-06","2026-11-05","2026-11-04","2026-10-01"],
              "reportDate":["2026-11-06","2026-11-03","2026-09-27","2026-10-01"],
              "form":["8-K","4","10-Q","8-K"],
              "items":["2.02,9.01","","",""],
              "primaryDocument":["aapl-20261106.htm","xslF345X05/form4.xml","aapl-20260927.htm","old.htm"],
              "primaryDocDescription":["8-K","FORM 4","10-Q","8-K"]}}}
            """;

    private HttpServer server;
    private final Map<String, String> pages = new HashMap<>();
    private final List<String> requested = new CopyOnWriteArrayList<>();
    private final List<String> userAgents = new CopyOnWriteArrayList<>();
    private final AtomicInteger throttled = new AtomicInteger();

    @BeforeEach
    void start() throws Exception {
        pages.put("/submissions/CIK0000320193.json", SUBMISSIONS);
        pages.put(DIR + "aapl-20261106.htm", "<html><ix:header><ix:hidden>hidden junk</ix:hidden></ix:header><body>"
                + "<p>Item 2.02 Results of Operations and Financial Condition.</p></body></html>");
        pages.put(DIR + "index.json", "{\"directory\":{\"item\":[{\"name\":\"aapl-20261106.htm\"},"
                + "{\"name\":\"a8-kex991q4.htm\"},{\"name\":\"R1.htm\"}],\"name\":\"" + DIR + "\"}}");
        pages.put(DIR + "a8-kex991q4.htm", "<html><body><p>Apple reports fourth quarter results. "
                + "Revenue of $94.9 billion, up 6 percent year over year.</p></body></html>");
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", ex -> {
            String path = ex.getRequestURI().getPath();
            requested.add(path);
            userAgents.add(String.valueOf(ex.getRequestHeaders().getFirst("User-Agent")));
            String body = pages.get(path);
            byte[] b = (body == null ? "not found" : body).getBytes(StandardCharsets.UTF_8);
            ex.sendResponseHeaders(body == null ? 404 : 200, b.length);
            ex.getResponseBody().write(b);
            ex.close();
        });
        server.start();
    }

    @AfterEach
    void stop() {
        server.stop(0);
    }

    private SecFilings sec(String userAgent) {
        String base = "http://127.0.0.1:" + server.getAddress().getPort();
        return new SecFilings(base, base, userAgent, throttled::incrementAndGet);
    }

    @Test
    void returnsRecentMaterialFilingsWithTextForEightK() throws Exception {
        List<SecFilings.Filing> out = sec(UA).recent("0000320193", LocalDate.parse("2026-11-03"));

        assertEquals(List.of("8-K", "10-Q"), out.stream().map(SecFilings.Filing::form).toList(), "表单 4 和太旧的不要");
        SecFilings.Filing k = out.get(0);
        assertEquals("2026-11-06", k.date());
        assertTrue(k.url().endsWith(DIR + "aapl-20261106.htm"), k.url());
        assertTrue(k.title().contains("8-K") && k.title().contains("2.02"), k.title());
        assertTrue(k.text().contains("Item 2.02 Results of Operations"), k.text());
        assertTrue(k.text().contains("Revenue of $94.9 billion"), "8-K 的数字在 EX-99 附件里:" + k.text());
        assertFalse(k.text().contains("hidden junk"), "内联 XBRL 的隐藏头不是正文");

        SecFilings.Filing q = out.get(1);
        assertTrue(q.url().endsWith("/Archives/edgar/data/320193/000032019326000099/aapl-20260927.htm"), q.url());
        assertEquals("", q.text(), "10-Q 正文太长,不抓");
        assertEquals(4, requested.size(), "submissions + 8-K 正文 + 目录 + 附件;10-Q 不抓正文:" + requested);
        assertEquals(4, throttled.get(), "每个请求都过限速(SEC 上限 10 次/秒)");
        assertTrue(userAgents.stream().allMatch(UA::equals), "SEC 要求每个请求都带 User-Agent:" + userAgents);
    }

    @Test
    void aMissingExhibitStillLeavesTheFiling() throws Exception {
        pages.remove(DIR + "index.json");
        SecFilings.Filing k = sec(UA).recent("0000320193", LocalDate.parse("2026-11-05")).get(0);
        assertTrue(k.text().contains("Item 2.02"), k.text());
        assertFalse(k.text().contains("Revenue"));
    }

    @Test
    void withoutUserAgentItIsDisabledAndFetchesNothing() throws Exception {
        SecFilings off = sec("  ");
        assertFalse(off.enabled());
        assertTrue(sec(UA).enabled());
        assertEquals(List.of(), off.recent("0000320193", LocalDate.parse("2026-11-03")));
        assertTrue(requested.isEmpty());
    }

    @Test
    void rejectsACikThatIsNotTenDigits() {
        assertThrows(IllegalArgumentException.class, () -> sec(UA).recent("320193/../x", LocalDate.parse("2026-11-03")));
        assertThrows(IllegalStateException.class, () -> sec(UA).recent("0000000001", LocalDate.parse("2026-11-03")),
                "查不到这家公司(404)要报错,不能当成'没有公告'");
    }
}
