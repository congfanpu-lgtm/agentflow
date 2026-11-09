package com.agentflow.worker.brief;

import com.agentflow.worker.agent.TextUtil;
import com.agentflow.worker.agent.tool.WebFetchTool;
import com.agentflow.worker.llm.RateLimiter;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * 美国上市公司在 SEC(证监会)EDGAR 上的公告:业绩、并购、高管变动都得先在这里披露,是比新闻更早、更准的一手来源。
 * 只读两个固定域名(data.sec.gov 的公告列表、www.sec.gov 的公告正文),路径里的每一段都校验过格式,不存在 SSRF 面。
 *
 * <p>SEC 的两条硬规矩:请求必须带能联系到人的 User-Agent(否则 403),全站不超过 10 次/秒。
 * 没配 {@code SEC_USER_AGENT} 时整个来源关闭,简报只用新闻搜索。
 */
@Slf4j
@Component
public class SecFilings {

    /** 一条公告。text 只有 8-K / 6-K 才抓(正文短,数字在附件 EX-99 新闻稿里);抓不到就是空串。 */
    public record Filing(String form, String date, String title, String url, String text) {}

    static final int MAX_FILINGS = 3;
    private static final int MAX_BYTES = 4_000_000;
    private static final int MAIN_CHARS = 1500;
    private static final int EXHIBIT_CHARS = 3500;
    /** 8-K 临时公告、10-Q 季报、10-K 年报;6-K / 20-F 是外国公司(中概股)对应的表单。 */
    private static final Set<String> FORMS = Set.of("8-K", "10-Q", "10-K", "6-K", "20-F");
    private static final Set<String> WITH_TEXT = Set.of("8-K", "6-K");
    private static final Pattern CIK = Pattern.compile("\\d{10}");
    private static final Pattern ACCESSION = Pattern.compile("\\d{10}-\\d{2}-\\d{6}");
    private static final Pattern FILE_NAME = Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]*");
    private static final Pattern EXHIBIT_99 = Pattern.compile("(?i).*ex(hibit)?[-_]?99.*\\.htm");
    private static final ObjectMapper OM = new ObjectMapper();

    private final String dataBase;
    private final String wwwBase;
    private final String userAgent;
    private final Runnable throttle;
    private final HttpClient http = HttpClient.newBuilder()
            .followRedirects(HttpClient.Redirect.NEVER)
            .connectTimeout(Duration.ofSeconds(5)).build();

    @Autowired
    public SecFilings(@Value("${agent.sec.data-base-url:https://data.sec.gov}") String dataBase,
                      @Value("${agent.sec.www-base-url:https://www.sec.gov}") String wwwBase,
                      @Value("${agent.sec.user-agent:${SEC_USER_AGENT:}}") String userAgent,
                      RateLimiter limiter) {
        this(dataBase, wwwBase, userAgent, () -> acquire(limiter));
    }

    SecFilings(String dataBase, String wwwBase, String userAgent, Runnable throttle) {
        this.dataBase = dataBase;
        this.wwwBase = wwwBase;
        this.userAgent = userAgent == null ? "" : userAgent.trim();
        this.throttle = throttle;
    }

    /** 所有 worker 共用一个桶(Redis),8 次/秒,给 SEC 的 10 次/秒上限留余量。 */
    private static void acquire(RateLimiter limiter) {
        for (int i = 0; i < 100; i++) {
            if (limiter.tryAcquire("sec", 8, 8.0)) {
                return;
            }
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("等待 SEC 限速时被中断");
            }
        }
        throw new IllegalStateException("等待 SEC 限速超时");
    }

    public boolean enabled() {
        return !userAgent.isEmpty();
    }

    /** since(含)之后的重要公告,最新的在前,最多 {@link #MAX_FILINGS} 条。cik 是 10 位、左侧补零的公司编号。 */
    public List<Filing> recent(String cik, LocalDate since) throws IOException, InterruptedException {
        if (cik == null || !CIK.matcher(cik).matches()) {
            throw new IllegalArgumentException("cik 必须是 10 位数字");
        }
        if (!enabled()) {
            return List.of();
        }
        JsonNode recent = OM.readTree(get(dataBase + "/submissions/CIK" + cik + ".json")).path("filings").path("recent");
        JsonNode forms = recent.path("form");
        List<Filing> out = new ArrayList<>();
        for (int i = 0; i < forms.size() && out.size() < MAX_FILINGS; i++) {
            String form = forms.get(i).asText();
            String date = recent.path("filingDate").path(i).asText("");
            String accession = recent.path("accessionNumber").path(i).asText("");
            String doc = recent.path("primaryDocument").path(i).asText("");
            if (!FORMS.contains(form) || date.compareTo(since.toString()) < 0
                    || !ACCESSION.matcher(accession).matches() || !FILE_NAME.matcher(doc).matches()) {
                continue;
            }
            String dir = wwwBase + "/Archives/edgar/data/" + Long.parseLong(cik) + "/" + accession.replace("-", "") + "/";
            String items = recent.path("items").path(i).asText("");
            String period = recent.path("reportDate").path(i).asText("");
            String title = !items.isEmpty() ? form + " (Items " + items.replace(",", ", ") + ")"
                    : !period.isEmpty() ? form + " (报告期 " + period + ")" : form;
            out.add(new Filing(form, date, title, dir + doc, WITH_TEXT.contains(form) ? text(dir, doc) : ""));
        }
        return out;
    }

    /** 公告正文 + 第一份 EX-99 附件(业绩新闻稿)。任何一步失败都保留已拿到的部分:公告本身(表单、日期、链接)仍然有用。 */
    private String text(String dir, String doc) throws InterruptedException {
        String text = "";
        try {
            text = TextUtil.truncate(readable(dir + doc), MAIN_CHARS);
            for (JsonNode item : OM.readTree(get(dir + "index.json")).path("directory").path("item")) {
                String name = item.path("name").asText("");
                if (!name.equals(doc) && EXHIBIT_99.matcher(name).matches() && FILE_NAME.matcher(name).matches()) {
                    return text + "\n" + TextUtil.truncate(readable(dir + name), EXHIBIT_CHARS);
                }
            }
        } catch (IOException | RuntimeException e) {
            log.warn("SEC 公告正文没抓全 {}{}: {}", dir, doc, e.toString());
        }
        return text;
    }

    private String readable(String url) throws IOException, InterruptedException {
        // 内联 XBRL 的 <ix:header> 是给机器读的隐藏数据,不是正文
        return WebFetchTool.htmlToText(get(url).replaceAll("(?is)<ix:header>.*?</ix:header>", " "));
    }

    private String get(String url) throws IOException, InterruptedException {
        throttle.run();
        HttpRequest req = HttpRequest.newBuilder(URI.create(url)).GET()
                .header("User-Agent", userAgent)
                .timeout(Duration.ofSeconds(15)).build();
        HttpResponse<InputStream> resp = http.send(req, HttpResponse.BodyHandlers.ofInputStream());
        try (InputStream in = resp.body()) {
            if (resp.statusCode() / 100 != 2) {
                throw new IllegalStateException("SEC HTTP " + resp.statusCode());
            }
            return new String(in.readNBytes(MAX_BYTES), StandardCharsets.UTF_8);
        }
    }
}
