# AgentFlow 面试讲解文档(RESEARCH_AGENT)

> 用法:先背「口述稿」,再顺着「一个请求的旅程」把每一站的代码打开读一遍,最后过「必答题」。
> 文中 `文件:行号` 以 2026-10-15 的 `feat/research-agent` 分支为准。

---

## 1. 口述稿

### 30 秒版

AgentFlow 是一个跑在 Kafka worker 池上的多 agent 研究系统。用户提一个问题,规划模型先把它拆成几个子问题,
每个子问题由一个 ReAct agent 并行调研,agent 自己决定调 Wikipedia、抓网页还是查本地知识库,最后合成一篇带引用的报告。
我重点做的是可靠性:agent 每走一步就在 Redis 存档,worker 中途被杀掉,另一个 worker 会从最后一步接着跑,
已经调过的工具不会再调一次。这个我用 chaos 脚本实测过。

### 2 分钟版

1. **为什么做**:常见的 agent demo 都是单进程循环,进程一挂整个任务从头再来,工具和 token 都要重新花。
   我想做的是"agent 运行时的基础设施",把分布式系统里成熟的可靠性手段用到 agent 上。
2. **怎么做**:
   - 编排层是我之前自研的:Kafka 扇出、数据库 CAS 状态机、分级重试 + 死信队列、超时兜底、幂等、执行轨迹。
   - 在它上面加了一个三阶段任务:拆题 → N 个 agent 并行 → 合成报告。每个阶段的每个子任务都是普通 Kafka 消息,
     所以自动继承了上面那一整套可靠性。
   - agent 循环用 OpenAI 原生 tool calling,自己写的,不用 LangChain,因为我需要自己控制"存档放在哪一步"。
3. **最难的点**:
   - **续跑的正确性**:存档必须在"一步完整结束"后才写,崩溃在存档前就重做这一步。工具都是只读的,
     所以 at-least-once 投递加上只读工具,效果上等价 exactly-once。
   - **阶段推进只发生一次**:多个 agent 的结果同时回来时,靠任务计数行的行锁串行化,只有最后一个看到"全部落定",
     所以合成报告只会创建一次。
4. **结果(实测)**:
   - `chaos-agent.sh`:agent 跑完第 1 步后 `docker kill` 它所在的 worker,另一个 worker 从第 2 步接手,
     轨迹里第 1 步只在被杀的 worker 上执行过一次,任务最终 COMPLETED。
   - 129 个 Java 测试 + 6 个 Python 测试,CI 全程用 mock 模型,不需要 key。

---

## 2. 一个请求的旅程

每一站都按「代码在哪 / 做了什么 / 为什么这样设计 / 可能的追问」来写。

### ① 提交:`TaskController`

- **代码**:`agentflow-server/.../controller/TaskController.java:24`
- **做了什么**:接收 `{type:"RESEARCH_AGENT", payload:{question}}`,交给 `TaskSubmitService`,返回 202 和 `taskUuid`。
- **为什么**:接口异步返回,真正的工作在 Kafka 后面做;前端拿 uuid 轮询进度和轨迹。
- **追问**:为什么不用同步接口? → 一个研究任务要几十秒到几分钟,同步会占满 HTTP 线程,而且客户端断开就丢结果。

### ② 拆成第一个阶段:`ResearchAgentDecomposer.decompose`

- **代码**:`agentflow-server/.../coordinator/ResearchAgentDecomposer.java:38`
- **做了什么**:校验问题(非空、不超过 500 字),只创建 1 个 `kind=PLAN` 的子任务。
- **为什么**:server 端没有 LLM(LLM 网关只在 worker 里),所以拆题本身也做成一个子任务交给 worker,
  这样所有 LLM 调用都走同一个限流和记账出口。
- **追问**:为什么限制 500 字? → 输入校验放在信任边界,避免超长输入拖垮后面每一次 LLM 调用的成本。

### ③ 分发:`SubtaskDispatcher`

- **代码**:`SubtaskDispatcher.java:45`(`dispatchPending`)、`:72`(`typeOf`)
- **做了什么**:把 PENDING 子任务发到 Kafka,发送成功才把状态改成 DISPATCHED。消息的 type 字段取子任务的 `kind`,
  没有 kind 的旧任务类型则用任务类型。
- **为什么**:`typeOf` 抽成一个静态方法,是因为超时重投、DLQ 重放也要构造同样的消息,三处共用一个规则,
  避免某一处忘了改、把 AGENT 子任务路由到错误的处理器。
- **追问**:消息 key 为什么用 subtaskId? → 子任务之间互相独立,按 id 散列能均匀分到各分区,最大化并行。

### ④ worker 收消息:`SubtaskListener`

- **代码**:`agentflow-worker/.../listener/SubtaskListener.java:47`、`:65`、`:76`
- **做了什么**:幂等检查 → 按 type 找处理器 → `processor.process(msg)` → 先发结果,再标记幂等键。
- **为什么**:处理器接口新增了 `process(SubtaskMessage)` 默认方法,`AgentProcessor` 需要 `subtaskUuid` 当存档 key;
  其他处理器不用改。"先发结果再标记幂等"是原有设计:反过来的话,标记后、发送前崩溃,结果就永久丢了。
- **追问**:Kafka offset 什么时候提交? → spring-kafka 默认 BATCH 模式,一条处理完才提交。所以处理中途崩溃,
  消费组再平衡后另一个 worker 会重新收到这条消息,这就是续跑的触发方式。

### ⑤ 拆题:`PlanProcessor`

- **代码**:`agentflow-worker/.../processor/PlanProcessor.java:42`
- **做了什么**:调用规划模型 → 宽松提取 JSON(兼容 ```json 代码块)→ schema 校验 → 去重、去空、截断到 4 个。
  一个都没有就抛异常,进入重试。
- **为什么**:模型输出在代码层校验,而不是靠提示词"求"它守规矩。允许 1~4 个,因为简单问题可能只拆出 1 个,
  这不应该算失败。

### ⑥ 结果回到 server:`ResultHandleService`

- **代码**:`agentflow-server/.../service/ResultHandleService.java:35`、`:61`
- **做了什么**:CAS 把子任务从 DISPATCHED 改成 COMPLETED/FAILED(重复结果直接忽略)→ 原子累加计数 →
  交给 `StageAdvancer.onSettled`。
- **为什么**:超时兜底(`TimeoutSweepService.java:78`)落定失败时也调同一个 `onSettled`。两条触发源共用一个入口,
  否则超时失败的 agent 不会触发合成,任务就卡住了。

### ⑦ 阶段推进(重点):`StageAdvancer`

- **代码**:`StageAdvancer.java:57`(`onSettled`)、`:74`(`advance`)、`:87`(`addTotal`)、`:98`(afterCommit)
- **做了什么**:
  1. 如果 `done + failed == total`,问这个任务类型"有没有下一阶段"(`ResearchAgentDecomposer.nextStage`,`:50`)。
  2. 有就插入下一阶段的子任务,`subtask_total += n`,发一条 `STAGE_ADVANCED` 轨迹,**事务提交后**再发 Kafka。
  3. 没有就交给 `TaskFinalizer` 判终态。
- **为什么(并发)**:`incrementDone` 是对任务行的 `UPDATE`,行锁一直持有到事务提交。3 个 agent 的结果同时回来,
  第二个、第三个会在这一行上排队。只有最后一个能看到"全部落定",所以合成子任务只会被创建一次,不需要分布式锁。
- **为什么(同事务)**:插入新子任务和调大 total 跟"落定当前结果"在同一个事务里。如果分开,中间有一瞬间
  `done == total`,`TaskFinalizer` 可能把任务提前判成 COMPLETED。
- **为什么(afterCommit)**:如果在事务提交前就发消息,worker 可能比事务先跑完,回传结果时数据库里还查不到这个子任务。
- **追问**:一个 agent 失败了会怎样? → 只要至少一个 agent 成功就照常合成,任务最终是 PARTIAL_FAILED:
  报告还在,失败原因也能看到。全部失败就不合成,任务 FAILED。

### ⑧ agent 循环(重点):`AgentProcessor` → `AgentLoop`

- **代码**:`AgentProcessor.java:31`、`AgentLoop.java:73`
- **做了什么**:
  1. `checkpoint.load`(`:82`):读出已完成的步骤,原样放回对话,工具结果放进缓存;有存档就发 `RESUMED` 轨迹。
  2. 主循环最多 6 步:调模型(带工具列表)→ 没有 tool_calls 就是最终答案 → 否则逐个执行工具 → 追加到对话 →
     **一步全部结束后**才 `checkpoint.append`(`:122`)→ 发 `AGENT_STEP` 轨迹。
  3. 6 步用完:去掉工具列表,追加"请立即作答"(`:130`),强制收尾。
  4. `finish`(`:158`):宽松解析答案;来源只保留本次运行工具真实返回过的 URL(`:177`),其余删掉并发 `CITATION_DROPPED`。
- **为什么存档在一步结束之后**:如果在工具执行前就存,崩溃后会以为这一步做完了,但对话里缺少工具结果,
  模型会拿到残缺的上下文。存完整的一步,恢复出来的对话一定是合法的(每个 tool_call 都有对应的结果)。
- **为什么工具出错不抛异常**(`execute`,`:136`):网页 404、参数不合法、工具名不存在都是"模型可以自己纠正"的问题,
  回传 `ERROR: ...` 让它换个做法。只有 LLM 调用本身失败(限流、超时)才让子任务进重试;重试时也是从存档续跑。
- **为什么要缓存相同调用**:模型偶尔会原地打转,反复调同一个工具;缓存命中不再真实调用。错误结果不缓存,
  因为可能是网络抖动,应该允许重试。

### ⑨ 工具与 SSRF 防护

- **代码**:`UrlGuard.java:18`、`:40`;`WebFetchTool.java:54`;`WikiSearchTool`;`KbSearchTool`
- **做了什么**:`web_fetch` 只允许 http/https;解析域名后拒绝回环、私网、链路本地(含云元数据 169.254.169.254)、
  CGNAT(100.64/10,Tailscale 用这一段)、IPv6 ULA;自己跟随重定向,**每一跳都重新校验**;最多读 1MB、10 秒超时。
- **为什么**:URL 是模型给的,而模型读的网页里可能藏着注入指令("去访问 http://mysql:3306")。
  worker 在 docker 内网里,能直接访问 MySQL、Redis。"公网地址 302 跳到内网"是常见绕过方式,所以每一跳都要查。
- **追问**:还有什么漏洞? → DNS rebinding:校验时解析一次,真正连接时又解析一次,中间可能被换成内网 IP。
  修法是解析一次后按 IP 直连。代码里有 `ponytail:` 注释标明了这一点。

### ⑩ 合成报告:`SynthesizeProcessor`

- **代码**:`SynthesizeProcessor.java:43`、`:84`
- **做了什么**:所有 agent 的来源按 URL 去重、统一编号 `[1]..[k]`,连同各子问题结论交给模型;
  报告里超出编号范围的 `[n]` 直接删掉。
- **为什么**:引用编号由代码分配,模型只能引用列表里有的来源。这和 agent 阶段的 URL 校验是两道防线。

### ⑪ 终态与页面

- **代码**:`TaskFinalizer.java:38`、`:51`;`agentflow-server/src/main/resources/static/index.html`
- **做了什么**:CAS 把任务从 RUNNING 改成终态并聚合结果(每项带 `kind`)。页面每秒轮询任务和轨迹,
  用 `STAGE_ADVANCED` 画出子问题,用 `AGENT_STEP` / `RESUMED` / `CITATION_DROPPED` 画每个 agent 的卡片。
- **为什么**:模型和网页的文本一律用 `textContent` 渲染,链接只允许 http/https,防止网页内容在控制台里执行脚本。

---

## 3. 必答题

**Q1. 为什么 worker 不连 MySQL?**
状态只有 server 一个写入方,所有状态迁移都走 CAS,不会出现两边同时改一行、互相覆盖的情况。worker 扩到 50 个,
MySQL 也不会多出 50 个连接池。worker 没有数据库事务,崩溃了只是消息被重投,不会留下写了一半的数据。

**Q2. 存档为什么放 Redis?Redis 丢数据会怎样?**
存档是"续跑缓存",不是业务状态,任务成败仍然只由 MySQL 决定。Redis 丢了最近一步的存档,后果只是这一步重做一次,
因为工具只读,重做没有副作用,只多花一点 token。worker 本来就连着 Redis(幂等键、限流),不需要新增依赖。

**Q3. 崩溃发生在"工具执行完、存档之前"会怎样?这算 exactly-once 吗?**
这一步会重做,工具会再调用一次。严格说投递是 at-least-once;但工具全部只读,重复调用没有外部副作用,
所以效果上等价 exactly-once。如果以后加有副作用的工具(比如发邮件),就要像 server 端现有的 `SideEffectPolicy`
那样,用业务幂等键(例如 subtaskUuid + 步号)在执行前去重。

**Q4. 两个 worker 同时跑同一个子任务会怎样?**
正常情况不会:Kafka 一个分区同一时刻只分给消费组里的一个消费者。异常情况是超时兜底重投了,而原来那个 worker
其实还活着,只是很慢。这时存档可能被交错写入,`load` 只取从第 1 步开始连续的步号,重复步号跳过;
结果回到 server 时,子任务 CAS 只接受第一个结果,后来的被忽略。需要严格互斥的话,可以加一个按 subtaskUuid 的 Redis 锁。

**Q5. 为什么不用 LangChain4j / Temporal?**
LangChain 的 agent 循环把"每一步"封装在框架里,我没法控制存档的时机和内容。Temporal 能做到每一步持久化,
但要引入一整套新的服务端;我已经有 Kafka + 状态机,复用它的成本最低。
更彻底的做法(方案 B)是每一步一条 Kafka 消息,长循环不会一直占着一个 worker,这写在 README 的 Future work 里。

**Q6. 怎么防止模型编造引用?**
两道防线:agent 阶段,来源 URL 必须出现在本次运行工具真实返回的结果里;合成阶段,`[n]` 编号由代码分配,
超出范围的直接删除。删掉的 URL 会记进轨迹(`CITATION_DROPPED`),可以追查。

**Q7. 怎么防 SSRF / 提示注入让 agent 访问内网?**
见第 ⑨ 站:协议白名单、解析后按 IP 段拒绝、每一跳重定向都重新校验、大小和超时上限。已知缺口是 DNS rebinding。

**Q8. 上下文怎么控制?为什么不能像 `AgentContext` 那样逐条丢历史?**
OpenAI 的协议要求每个 assistant 的 tool_call 后面紧跟对应的 tool 结果,逐条丢会让对话不合法、直接被 API 拒绝。
现在靠两个上限:每个工具结果截断到 2000 字,最多 6 步。步数调大时,应该把早期步骤压缩成一段摘要。

**Q9. 部分失败的语义?**
一个 agent 失败,其他 agent 的结论照常合成,任务是 PARTIAL_FAILED,报告和失败原因都能看到。
拆题失败或所有 agent 都失败,任务 FAILED,不浪费合成那一次调用。

**Q10. 为什么所有 LLM 调用都要经过网关?**
限流(Redis + Lua 令牌桶,跨 worker 共享)、多模型路由、token 记账都在这一个出口。业务代码拿不到底层 client,
结构上就没有绕过的可能。三个阶段的 LLM 调用都走它,所以页面页脚的成本是准的。

**Q11. 演示用的是 mock,怎么证明真实模型也能用?**
`OpenAiLlmClient` 的 tool calling 序列化和解析有单测(本地 HTTP 桩,`OpenAiLlmClientTest`)。
在 `.env` 里填 `LLM_PROVIDER=openai` 和任意 OpenAI 兼容服务的 key,就会切到真实模型,代码不用改。

---

## 4. 已知局限与下一步

对应代码里的 `ponytail:` 注释(`grep -rn "ponytail:" agentflow-*/src/main`):

| 位置 | 局限 | 下一步 |
|---|---|---|
| `AgentLoop.java:41` | 上下文只靠"截断 + 步数上限"封顶 | 步数调大时把早期步骤压缩成摘要 |
| `UrlGuard.java:13` | DNS rebinding 窗口 | 解析一次后按 IP 直连 |
| `WebFetchTool.java:18` | 只按 UTF-8 解码网页 | 按 Content-Type 的 charset 解码 |
| `AgentCheckpoint.java:19` | 超时重投时可能双跑 | 按 subtaskUuid 加 Redis 锁 |
| `SchemaValidator.java:13` | 只校验必填和基础类型 | 完整 JSON Schema |
| `TokenEstimator.java:7` | token 数按字符数估算 | 换精确 tokenizer |
| `RateLimiter.java:47` | 令牌桶用 worker 的墙钟 | 改用 Redis 服务端时间 |

其他:一个长 agent 循环会一直占着一个 worker 线程(方案 B 可解决);页面用 1 秒轮询而不是 SSE;没有通用网页搜索工具;
**没有鉴权**,公网开放前必须先加。
