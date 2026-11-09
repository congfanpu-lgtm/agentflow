# 股票每日简报 v1 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 在 AgentFlow 引擎上加一层面向个人投资者的产品:注册登录 → 加自选股 → 每天自动生成带出处的中文简报 → 网页查看 + 按用户设定的时间推送到飞书 / Slack。

**Architecture:** 不改引擎。新任务类型 `DAILY_BRIEF`(server 端 `DailyBriefDecomposer`,每只股票一个 `BRIEF` 子任务)+ worker 端 `BriefProcessor`(SEC 公告 + 新闻搜索 → 一次 LLM 调用 → 校验后的事件列表)。任务落定时事件写入 `stock_event` 表(同时是时间线和简报内容)。推送由每小时一次的 `DeliveryService` 完成,`push_log` 唯一键保证每人每件事只推一次。

**Tech Stack:** 现有栈(Java 17 语法、Spring Boot 3.3、MyBatis-Plus、Redisson、Kafka、MySQL);不加任何新依赖。密码用 JDK 自带 PBKDF2,会话存 Redis,页面是一个原生 JS 的静态页。

**Spec:** `docs/product-framework.md`(产品框架;本计划实现其中"第一版"的自选股 + 每日简报 + 推送,公司档案页、追问对话、财报深挖不在本计划内)

## Global Constraints

- 不新增 Maven 依赖;不用 Java 21 才有的 API(CI 是 Java 17)。
- 不给买卖建议、不预测股价:prompt 里写明,页面和推送都带"仅为信息整理,不构成投资建议"。
- 每个事件必须带出处链接;链接只取自我们自己抓到的条目(按编号),从不取模型输出里的 URL。
- 引用原文(`quote`)必须能在条目正文里逐字找到,否则丢弃引用(事件保留)。
- 用户提供的 webhook 地址只允许 `https://hooks.slack.com/services/…`、`https://open.feishu.cn/open-apis/bot/v2/hook/…`、`https://open.larksuite.com/open-apis/bot/v2/hook/…`;不跟随重定向;接口从不返回完整地址。
- 登录态校验写在每个接口里(`sessions.require(req)`),不靠路径前缀过滤器。
- 引擎原有接口(`/api/v1/tasks` 等)在公开部署时仍由 `DemoGuard` 的访问码保护;只有 `^/api/v1/(auth|me)(/[a-z-]+)*$` 这种原样路径被放行。
- SEC 接口要求 User-Agent 带联系方式:由部署者通过 `SEC_USER_AGENT` 提供;未配置时产品只用新闻源,自选股代码只做格式校验。
- 新测试沿用现有方式:`@SpringBootTest` 连本机中间件;HTTP 外部依赖用 `com.sun.net.httpserver.HttpServer` 本地桩。

## Review Focus

1. 两个用户之间的数据隔离:A 看不到 B 的自选股 / 设置,删不掉 B 的自选股 → `ProductApiTest.usersAreIsolated`。
2. 同一件事重复推送:投递任务重跑、多实例同时跑 → `DeliveryServiceTest.eachEventIsPushedOncePerUser`;推送失败后下次能补发 → `failedPushIsRetriedNextTime`。
3. 新闻正文里的指令或伪造链接:模型返回不存在的条目编号 / 编造引用 → `BriefProcessorTest.dropsEventsWithoutAValidSource`、`dropsQuotesThatAreNotInTheSource`。
4. webhook 地址被用来打内网:`http://`、IP、带 `@`、相似域名、其他路径 → `WebhookClientTest.rejectsEverythingButTheThreeKnownHosts`。
5. 访问码绕过:`/api/v1/me/../tasks`、`/api/v1/me;x/…`、`/api/v1/%6de` 不能借产品路径的放行规则跳过访问码 → `DemoGuardTest.productPathsSkipTheCodeButLookalikesDoNot`。

---

### Task 1: 表结构、实体、Mapper

**Files:**
- Modify: `docker/mysql/init.sql`(追加 4 张表)
- Create: `docker/mysql/migrations/2026-11-09-stock-brief.sql`(同样的 4 张表,给已有数据卷的环境手动执行)
- Create: `agentflow-server/src/main/java/com/agentflow/server/entity/{UserEntity,WatchlistEntity,StockEventEntity}.java`
- Create: `agentflow-server/src/main/java/com/agentflow/server/mapper/{UserMapper,WatchlistMapper,StockEventMapper,PushLogMapper}.java`

**Interfaces — Produces:**
- `app_user(id, email UNIQUE, password_hash, webhook_url NULL, push_hour TINYINT DEFAULT 8, timezone DEFAULT 'Asia/Shanghai', created_at)`
- `watchlist(id, user_id, ticker, company_name, created_at, UNIQUE(user_id,ticker), KEY(ticker))`
- `stock_event(id, ticker, brief_date, event_date NULL, title, summary, importance, url, url_hash CHAR(64), source_title NULL, quote NULL, form NULL, created_at, UNIQUE(ticker,url_hash), KEY(ticker,created_at))`
- `push_log(id, user_id, event_id, created_at, UNIQUE(user_id,event_id))`
- `UserMapper.updateSettings(long id, String webhookUrl, int pushHour, String timezone)`(MyBatis-Plus 的 `updateById` 不会把字段写成 NULL,清空 webhook 需要显式 SQL)
- `StockEventMapper.insertIgnore(StockEventEntity) : int`
- `PushLogMapper.claim(long userId, long eventId) : int`(`INSERT IGNORE`,1 = 抢到)、`release(long userId, long eventId) : int`

- [ ] 写 SQL 与类;在本机库执行迁移;`mvn -q -pl agentflow-server -am compile` 通过。(本任务没有分支逻辑,由后续任务的测试覆盖。)
- [ ] Commit `feat: 股票简报的表结构与实体`

### Task 2: 账号与登录

**Files:**
- Create: `agentflow-server/src/main/java/com/agentflow/server/product/{PasswordHasher,Sessions,Throttle,AuthController,ProductErrors}.java`
- Modify: `agentflow-server/src/main/java/com/agentflow/server/demo/DemoGuard.java`
- Test: `agentflow-server/src/test/java/com/agentflow/server/product/{PasswordHasherTest,ProductApiTest}.java`,`demo/DemoGuardTest.java`

**Interfaces — Produces:**
- `PasswordHasher.hash(String) : String`(格式 `pbkdf2$600000$<salt b64>$<hash b64>`)、`verify(String password, String stored) : boolean`
- `Sessions.create(long userId, HttpServletResponse)`、`resolve(HttpServletRequest) : Long`、`require(HttpServletRequest) : long`(未登录抛 401)、`destroy(req, resp)`;cookie `AF_SESSION`,HttpOnly,SameSite=Lax,30 天;Redis 键 `agentflow:session:<sha256(token)>`
- `Throttle.allow(String key, int max, Duration window) : boolean`
- `DemoGuard.clientKey(HttpServletRequest) : String`(限流用的客户端标识);`PUBLIC_PAGES` 加 `/app.html`;原样 URI 匹配 `^/api/v1/(auth|me)(/[a-z-]+)*$` 时放行
- `POST /api/v1/auth/register|login|logout`,请求体 `{email,password}`;密码 8–128 位;注册每地址每天 5 次,登录每地址 15 分钟 20 次、每邮箱 15 分钟 10 次(超出 429);登录失败统一返回"邮箱或密码错误";邮箱已注册 409
- `ProductErrors`:只作用于产品接口的 `@RestControllerAdvice`,把 `ResponseStatusException` 转成 `{"error": 原因}`

- [ ] `PasswordHasherTest`:同一密码两次哈希不同且都能校验通过;错误密码、被篡改的串、格式不对的串返回 false。
- [ ] `ProductApiTest`:注册后带 cookie 访问 `/api/v1/me` 得到邮箱;不带 cookie 401;重复注册 409;错误密码 401;登出后 cookie 失效;同一邮箱连续 11 次错误登录后 429。
- [ ] `DemoGuardTest.productPathsSkipTheCodeButLookalikesDoNot`:开着访问码时 `/api/v1/me` 返回 401 且提示是"请先登录"(说明过了访问码这一关),`/app.html` 200;`/api/v1/me/../tasks`、`/api/v1/me;x/watchlist`、`/api/v1/%6de` 仍要求访问码。
- [ ] 实现,跑通,Commit `feat: 账号注册登录(PBKDF2 + Redis 会话)`

### Task 3: 自选股与设置接口

**Files:**
- Create: `agentflow-server/src/main/java/com/agentflow/server/product/{TickerDirectory,WebhookClient,MeController}.java`
- Test: `product/{TickerDirectoryTest,WebhookClientTest}.java`,`ProductApiTest` 追加

**Interfaces — Produces:**
- `TickerDirectory.find(String ticker) : Optional<Company>`,`record Company(String ticker, String cik, String name)`;数据来自 `agentflow.sec.tickers-url`(默认 `https://www.sec.gov/files/company_tickers.json`),内存缓存 24 小时;`SEC_USER_AGENT` 为空或拉取失败时返回未校验的 `Company(ticker, null, ticker)`
- `WebhookClient.validate(String url) : String`(不合规抛 `IllegalArgumentException`)、`send(String url, String text)`(失败抛 `IllegalStateException`);Slack 请求体 `{"text":…}`,飞书 `{"msg_type":"text","content":{"text":…}}`;文本里的 `<` `>` 转义,防止 `<!channel>` / `<at>` 注入
- `GET /api/v1/me`、`GET|POST|DELETE /api/v1/me/watchlist`、`GET /api/v1/me/events?days=7`(最多 30)、`PUT /api/v1/me/settings`、`POST /api/v1/me/webhook-test`(每用户每小时 5 次)
- 股票代码:转大写,`.` 换成 `-`,必须匹配 `^[A-Z]{1,5}(-[A-Z]{1,2})?$`;每人最多 20 只;重复添加不报错
- 设置:`webhookUrl` 不传 = 不改,空串 = 清除;`pushHour` 0–23;`timezone` 必须能被 `ZoneId.of` 解析

- [ ] `TickerDirectoryTest`(本地桩):能查到 `AAPL` 的 cik(补零到 10 位)和公司名;查不到返回 empty;没配 User-Agent 时返回未校验结果且不发请求。
- [ ] `WebhookClientTest.rejectsEverythingButTheThreeKnownHosts`(表驱动);`postsTheRightBodyPerPlatform`(本地桩收到的 JSON);飞书返回 `{"code":19001}` 视为失败。
- [ ] `ProductApiTest`:`usersAreIsolated`;第 21 只被拒;非法代码 400;设置保存后 `/me` 只返回打码后的地址;非法 webhook 400。
- [ ] 实现,跑通,Commit `feat: 自选股与推送设置接口`

### Task 4: worker 端生成简报

**Files:**
- Modify: `agentflow-worker/.../agent/Prompts.java`(`BRIEFER_TAG`、`BRIEFER`)、`llm/MockLlmClient.java`、`agent/tool/WebSearchTool.java`(新增 `search`)、`agent/tool/WebFetchTool.java`(`htmlToText` 改 public)
- Create: `agentflow-worker/.../brief/SecFilings.java`、`processor/BriefProcessor.java`
- Test: `brief/SecFilingsTest.java`、`processor/BriefProcessorTest.java`、`llm/MockLlmClientTest.java` 追加
- Modify: 4 个写死 `redis://localhost:6379` 的 worker 测试,改为读 `SPRING_DATA_REDIS_PORT`(默认 6379)

**Interfaces — Produces:**
- `WebSearchTool.search(String query, String range) : List<Hit>`,`record Hit(String title, String url, String snippet)`;`run` 的输出格式不变
- `SecFilings.enabled() : boolean`、`recent(String cik, LocalDate since) : List<Filing>`,`record Filing(String form, String date, String title, String url, String text)`;只看 8-K / 10-Q / 10-K / 6-K / 20-F,最新 3 份;8-K、6-K 抓正文和 EX-99 附件
- 子任务输入 `{ticker, cik?, name, date, seenUrls[], knownEvents[]}`
- 子任务输出 `{ticker, date, events:[{title, summary, importance(1-3), url, sourceTitle, quote?, form?, eventDate?}], warnings[], usage}`
- 所有来源都失败 → 抛异常(走重试);没有新条目 → 输出空 events 且不调用模型

- [ ] `BriefProcessorTest`:正常路径;`dropsEventsWithoutAValidSource`;`dropsQuotesThatAreNotInTheSource`;已见过的 URL 被过滤、过滤后为空时不调用模型;搜索失败且没有 SEC 时抛异常;搜索失败但 SEC 有结果时输出里带 warning。
- [ ] `SecFilingsTest`(本地桩):只取关注的表单类型、按日期过滤、拼出正确的文档 URL、8-K 带上 EX-99 正文、请求带配置的 User-Agent。
- [ ] `MockLlmClientTest`:`[briefer]` 分支返回的引用能通过逐字校验。
- [ ] 实现,跑通,Commit `feat: worker 生成单只股票的每日简报`

### Task 5: server 端拆分、落库、触发

**Files:**
- Modify: `coordinator/TaskDecomposer.java`(新增默认空方法 `onFinished`)、`service/TaskFinalizer.java`(落定后调用)
- Create: `product/DailyBriefDecomposer.java`、`product/BriefService.java`
- Modify: `product/MeController.java`(加自选股后触发该股票的首份简报)
- Test: `product/DailyBriefDecomposerTest.java`

**Interfaces — Produces:**
- `TaskDecomposer.onFinished(TaskEntity task, List<SubtaskEntity> subs)`:任务进入终态时在同一事务里调用,默认什么都不做
- `DailyBriefDecomposer`:type `DAILY_BRIEF`,payload `{tickers?:[…], date?:"YYYY-MM-DD"}`;按关注人数排序,最多 `agentflow.brief.max-tickers`(默认 200)只;`onFinished` 把成功子任务的事件 `INSERT IGNORE` 进 `stock_event`(校验 ticker 一致、URL 是 http(s)、字段截断)
- `BriefService.triggerDaily() : boolean`(幂等键 `brief-run:<日期>T<小时>`)、`triggerForTicker(String ticker) : boolean`(幂等键 `brief-ticker:<代码>:<日期>`,全站每天最多 `agentflow.brief.max-on-demand`(默认 300)次);日期按 `America/New_York`

- [ ] `DailyBriefDecomposerTest`:两个用户关注同一只股票只生成一个子任务;输入里带上该股票近 7 天的 seenUrls / knownEvents;没有自选股时抛 `IllegalArgumentException`;`onFinished` 写入事件、同一 URL 第二次不重复、ticker 不符或 URL 非 http 的事件被丢弃。
- [ ] 实现,跑通,Commit `feat: DAILY_BRIEF 任务类型与事件落库`

### Task 6: 推送与定时

**Files:**
- Create: `product/DeliveryService.java`、`product/BriefScheduler.java`
- Modify: `agentflow-server/pom.xml`(测试时 `agentflow.brief.scheduler-enabled=false`)
- Test: `product/DeliveryServiceTest.java`

**Interfaces — Produces:**
- `DeliveryService.deliverDue(Instant now) : int`(发出的消息数):只处理本地小时等于 `push_hour` 且配置了 webhook 的用户;取其自选股最近 36 小时的事件;逐条 `push_log.claim` 抢占后合并成一条消息;发送失败则 `release`
- `BriefScheduler`:`agentflow.brief.cron`(默认 `0 0 7,18 * * *`,时区 `America/New_York`)触发生成;每小时第 5 分钟触发投递

- [ ] `DeliveryServiceTest`(`@MockBean WebhookClient`):`eachEventIsPushedOncePerUser`;`failedPushIsRetriedNextTime`;不在推送小时的用户不发;没有新事件不发。
- [ ] 实现,跑通,Commit `feat: 按用户设定的时间推送简报`

### Task 7: 页面、配置、文档

**Files:**
- Create: `agentflow-server/src/main/resources/static/app.html`
- Modify: `docker-compose.yml`、`.env.example`(`SEC_USER_AGENT`、`PUBLIC_BASE_URL`、`AUTH_COOKIE_SECURE`)、`README.md`、`docs/product-framework.md`

- [ ] 页面:登录 / 注册、自选股、按日期分组的事件(重要度、摘要、原文引用、出处链接)、推送设置与测试按钮、免责声明;所有外部文本用 `textContent`,链接只接受 http(s)。
- [ ] 本机用 mock 模型跑通:注册 → 加股票 → 一分钟内页面出现事件。
- [ ] 全量 `mvn test` 通过;Commit `feat: 简报页面与部署配置`
