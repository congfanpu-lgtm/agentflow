# AgentFlow — 分布式多 Agent 任务编排平台 · 设计文档

> 创建:2026-07-11 · 状态:已定稿
> 作者:丛凡普(设计协作:Claude)
> 目标:2027 暑期实习(美国 SDE/AIE/MLE + 国内大厂 Agent/后端)的核心简历项目
> 时间预算:2026-07 ~ 2026-09(约 10 周,每周 10–20h,项目优先)

---

## 0. 一句话定位(双市场)

**🇺🇸 EN:** A distributed multi-agent task orchestration platform — decomposes complex
tasks into DAGs, dispatches them via message queue to horizontally-scalable agent
workers, with self-built idempotency, rate limiting, and a cost-aware LLM gateway.

**🇨🇳 中:** 高并发分布式 Agent 任务调度平台——协调器将复杂任务拆解为 DAG,经
RocketMQ 削峰分发至可水平扩展的 Agent Worker 池,自研幂等去重、令牌桶限流与
LLM 成本管控网关。

同一项目、两套叙事:美国面试讲**系统设计与可扩展性**(AI Platform / Agent Infra),
国内面试讲**高并发与工程深度**(削峰、幂等、分布式锁、限流)。

---

## 1. 为什么是这个项目(决策依据)

- **市场调研(2026-07)**:Agentic AI 岗位美国同比 +280%、中国 Agent 工程师 +310%,
  是两个市场增速最快的缺口;而纯 RAG 项目已被明确认定为"table stakes"、同质化重灾区。
- **技能契合**:任务调度/分布式锁/MQ/限流是本人 Java 分布式老本行的直接延伸,
  "backend engineer → Agent Infra" 是行业公认转型路径。
- **取代旧计划**:原 `distributed-search-rag` 计划(2026-06-19)方向撞同质化区,
  归档保留,其检索思想降级为本平台的一个内置工具服务。
- **框架调研结论**:无单一现成方案完全覆盖此形态;核心调度层自研(简历护城河),
  Agent 内循环与向量库用现成库(避免无意义造轮子)。

### 面试必答题(提前备好)

> **Q: 为什么不用 Temporal / LangGraph?**
> A: 调研过。选择自研核心调度层,是为了展示分布式系统设计能力而非框架使用能力;
> 同时控制依赖与复杂度,保证系统每个组件我都能从原理讲到实现。Agent 内部的
> LLM 循环用 LangChain4j——那部分是成熟能力,重写才是浪费。

### 撞脸规避

- ❌ 不看、不抄 nageoffer/ragent(付费社群八股项目,面试官见太多)
- ✅ 架构参考对象仅限公开设计思想:Temporal 的 Worker-TaskQueue-长轮询模型、
  AgentScope Java 2.0 的 Manager-Worker 架构、RocketMQ for AI 官方实践

---

## 2. 演示场景:批量深度调研任务

> 用户提交:"分析这 N 篇文档,生成对比报告" → 协调器拆解为 DAG
> (抓取 → 检索 → 逐篇分析 → 汇总)→ Worker 池并发执行 → 聚合输出报告。

选它的原因:天然高并发(N 个子任务)、天然需要限流(并发 LLM 调用)、
天然用到 RAG(检索工具)、结果可演示(真实报告,可截图/录 GIF)。

---

## 3. 架构

```
用户 → [API 服务] → [协调器 Coordinator] → RocketMQ → [Worker 池 ×N]
                         ↓                                ↓
                   MySQL + Redis(任务状态机)        [LLM 网关] → LLM API
                                                         ↓
                                                   [RAG 检索服务] → pgvector
```

### 组件与自研/用库边界

| 组件 | 职责 | 自研 or 用库 | 语言 |
|---|---|---|---|
| API 服务 | 提交任务、查进度、取结果 | SpringBoot 常规 | Java |
| **协调器** ⭐ | 任务拆解为 DAG、依赖管理、分发、结果聚合 | **纯自研**(卖点1) | Java |
| **任务状态机** ⭐ | pending/running/retry/failed/done + 超时兜底 + 死信队列 | **纯自研**(卖点2) | Java |
| **Worker 池** ⭐ | 消费任务、幂等去重(Redisson+MD5)、优雅上下线、水平扩展 | 骨架自研;Agent 内循环用 **LangChain4j** | Java |
| **LLM 网关** ⭐ | **Redis+Lua 自实现令牌桶**、多模型路由、token 记账 | **纯自研**(卖点3) | Java |
| RAG 检索服务 | embedding + pgvector 检索 + 简单混合召回,作为 Agent 工具 | 向量库用现成,链路自搭,**不卷复杂 RAG** | **Python/FastAPI** |

### 技术栈

- **Java 17 + SpringBoot 3**(主体 ~85%):API、协调器、状态机、Worker、LLM 网关
- **Python + FastAPI**(~15%):RAG 检索微服务(embedding 生态优势 + 跨语言架构加分)
- RocketMQ(任务分发)· Redis/Redisson(锁、限流、热状态)· MySQL(任务持久化)
- pgvector(向量检索)· LangChain4j(Agent 内循环)· Docker Compose(多容器编排)
- LLM:开发用便宜模型(qwen-turbo / gpt-4o-mini 级);压测用 **mock LLM**(测调度不烧钱)

**语言决策记录**:核心卖点是分布式调度 → 必须 Java(国内后端市场 + 本人最强栈);
RAG 独立成 Python 微服务 → 一石三鸟(AI 生态、跨语言微服务经验、美国 AIE 岗位友好)。

---

## 4. 必测指标(简历数字全部实测,保留测试脚本与记录)

1. **吞吐/延迟**:N 并发子任务下的 P99 延迟、任务/分钟(k6 或 JMeter)
2. **水平扩展**:Worker 1→3 台吞吐提升比例
3. **成本控制**:幂等去重拦截的重复 LLM 调用比例(节省 token %)
4. **容错**:压测中 kill 一个 Worker,任务零丢失(消息重投 + 幂等)
5. **检索质量**:RAG 工具 Recall@K(自建小标注集,几十条即可)

---

## 5. 错误处理设计

- Worker 崩溃 → MQ 消息重投 + 消费幂等(Redisson 锁 + 内容 MD5)兜底
- LLM 超时/失败 → 指数退避重试,超限进死信队列,任务标记 failed 并可人工重放
- 毒消息(反复失败)→ 死信队列隔离,不阻塞正常消费
- 任务超时 → 状态机定时扫描 running 超时任务,重新入队或标记失败
- 单 Agent 异常 → 隔离在子任务级,不级联拖垮整个 DAG(参考 RocketMQ for AI 思路)

---

## 6. 测试策略

- **单元测试**:状态机流转、DAG 拆解逻辑、令牌桶算法(核心自研部分必须有)
- **集成测试**:docker-compose 起全链路,端到端跑通示例任务
- **压测**:k6/JMeter + mock LLM,产出第 4 节全部指标
- **容错演练**:压测中手动 kill Worker,验证零丢失

---

## 7. 10 周计划(每两周一个可运行里程碑)

| 周 | 内容 | 里程碑 / 简历可写 |
|---|---|---|
| W1–2 | 骨架:API + 状态机 + RocketMQ 分发 + 单 Worker(echo Agent) | 调度主链路能跑 |
| W3–4 | 可靠性:幂等、重试/死信、超时兜底、多 Worker + 优雅上下线 | 幂等/容错/水平扩展 |
| W5–6 | LLM 网关(令牌桶+路由+token 记账)+ 真 Agent(LangChain4j) | 限流/成本管控/Agent |
| W7 | RAG 检索服务(FastAPI + pgvector + embedding) | RAG 工具接入 |
| W8 | 压测 + 容错演练,测出全部指标 | 全部量化数字 |
| W9 | 英文 README + 架构图 + demo GIF + 云部署(可选) | GitHub 门面 |
| W10 | 缓冲 + 中英简历 bullet 定稿 | 完整项目条目 |

**范围纪律**:新想法一律进 backlog,W10 前不碰。W6 结束时手里已有完整可写的
"分布式任务调度系统",中断也不空手。

---

## 8. 双市场包装

| | 🇺🇸 美国 | 🇨🇳 国内 |
|---|---|---|
| GitHub | 英文 README + 架构图 + demo GIF | 同 repo 附中文文档 |
| 简历措辞 | horizontal scaling, fault tolerance, cost-aware gateway, observability | 削峰填谷、幂等、分布式锁、令牌桶、死信队列、高并发 |
| 加分项 | 云部署(GCP/AWS 免费层)、Prometheus 基础指标 | QPS/P99 数字、Redis+Lua 原子性细节 |
| 面试准备 | "Why not Temporal?" 标准答案 | 项目实现与 JVM/JUC/Redis 八股互相印证 |

---

## 9. 风险与兜底

| 风险 | 兜底 |
|---|---|
| 范围膨胀(最大风险) | 严格按周计划;backlog 制度;每两周里程碑硬检查 |
| 撞脸八股项目 | 见第 1 节撞脸规避;设计走自己的,不看社群项目代码 |
| LLM 费用失控 | 开发用便宜模型,压测用 mock;网关本身就有 token 记账 |
| 中途有事中断 | 里程碑式推进,任意偶数周结束都有完整可写的东西 |
| AI 辅助与诚实性 | 代码本人写(Claude 做架构指导/代码评审);每个里程碑附"能讲清楚"自检 |

---

## 10. 项目原则(对齐求职诚信)

1. 所有简历数字来自实测,保留脚本与记录,面试可复现。
2. 每个组件完成时,必须能脱稿讲清:为什么这么设计、还有什么替代方案、坑在哪。
3. 本项目取代旧的 `distributed-search-rag` 计划;旧目录归档不删(学习价值保留)。
