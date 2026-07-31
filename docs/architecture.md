# AgentFlow 架构图

> GitHub 直接渲染 Mermaid;本图与 `docs/specs/2026-07-11-agentflow-design.md` 第 3 节对齐。
> 核心边界:**Harness(管控面)** 负责编排/状态/可靠性/可观测/副作用;**Runtime(执行面)** 只"想和做",
> 一切副作用与持久化回 Harness 经 Policy。

## 系统架构

```mermaid
flowchart TB
    user([用户 / Client])
    console[Web 控制台<br/>static/index.html]

    subgraph HARNESS["Harness · 管控面 (Java / Spring Boot)"]
        api["API 服务 (REST)"]
        coord["协调器 Coordinator<br/>拆解 → 分发 → 聚合"]
        finalizer["TaskFinalizer<br/>终态判定 + 部分失败语义"]
        sweep["超时兜底扫描"]
        policy["统一副作用 Policy<br/>业务幂等 (afterCommit)"]
        tracec["Trace 消费者"]
    end

    subgraph KAFKA["Kafka (KRaft)"]
        t_sub[["AGENTFLOW_SUBTASK (3 分区)"]]
        t_retry[["自研阶梯重试 5s/30s/5m + DLQ"]]
        t_res[["AGENTFLOW_RESULT"]]
        t_trace[["AGENTFLOW_TRACE"]]
    end

    subgraph RUNTIME["Runtime · 执行面 · Worker 池 ×N (Java)"]
        listener["SubtaskListener<br/>按 type 路由"]
        idem["幂等去重 (MD5 + Redis)"]
        proc["SubtaskProcessor<br/>Echo / LLM / Research"]
        ctx["AgentContext<br/>上下文裁剪防膨胀"]
        skill["Skill 注册 + schema 校验"]
        gw["LLM 网关<br/>Redis+Lua 令牌桶 · 路由 · 记账"]
    end

    rag["RAG 检索服务<br/>Python / FastAPI"]
    llm["LLM API<br/>mock (默认) / OpenAI 兼容"]

    mysql[("MySQL<br/>任务状态机 (CAS)")]
    redis[("Redis / Redisson<br/>锁 · 限流 · 记账 · 幂等")]
    mongo[("MongoDB<br/>Run Trace")]
    pg[("pgvector<br/>向量近邻检索")]

    user --> console --> api
    api --> coord
    coord --> mysql
    coord -->|分发| t_sub
    t_sub --> listener --> idem --> proc
    proc --> ctx --> gw --> llm
    proc --> skill
    gw --> redis
    idem --> redis
    proc -->|意图命中才检索| rag --> pg
    proc -->|成功| t_res --> finalizer
    proc -.失败.-> t_retry -.重投.-> t_sub
    t_retry -.耗尽.-> finalizer
    sweep -.卡死重投/落定.-> finalizer
    finalizer --> mysql
    finalizer --> policy
    coord & proc & finalizer -->|埋点| t_trace --> tracec --> mongo
    api -->|查询/回放| mysql & mongo

    classDef store fill:#1c2a3a,stroke:#4493f8,color:#e6edf3;
    classDef ext fill:#2a2140,stroke:#7c5cff,color:#e6edf3;
    class mysql,redis,mongo,pg store;
    class rag,llm ext;
```

## 五存储 polyglot persistence(按访问模式选存储)

| 数据 | 访问模式 | 选型 |
|---|---|---|
| 任务状态机 | 事务性状态迁移、按 status/time 扫超时 | **MySQL**(CAS) |
| 任务分发队列 | 持久化日志、分区、消费组、可重放 | **Kafka**(KRaft) |
| 锁/限流/记账/幂等键 | 毫秒读写、TTL、原子操作 | **Redis / Redisson** |
| Run Trace | 嵌套 JSON、写多、按 traceId 整取回放 | **MongoDB** |
| RAG 向量 | 近邻检索(ANN) | **pgvector** |

## 自研核心(简历卖点)

1. **协调器 + 状态机**:DAG 拆解、部分失败语义(`PARTIAL_FAILED`)、CAS 状态迁移、超时兜底。
2. **Kafka 可靠性层**:自研阶梯延迟重试 topic + DLQ + 恢复重放(补齐 Kafka 无原生延迟/死信)。
3. **LLM 网关**:Redis+Lua 令牌桶限流 + 多模型路由 + token 记账,唯一出口收口。
4. **Run Trace**:事件溯源经 Kafka → MongoDB,全链路留痕、可回放审计"闭环执行成功"。
5. **统一副作用 Policy**:所有副作用经同一闸 + 业务幂等 + afterCommit,代码层 enforce。
