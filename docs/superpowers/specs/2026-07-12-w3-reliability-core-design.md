# W3 可靠性核心 · 设计文档(子计划一)

> 创建:2026-07-12 · 状态:已定稿(brainstorming 通过)
> 上游 spec:`docs/specs/2026-07-11-agentflow-design.md`(W3–4 行)
> 前置:W1–2 骨架已合并入 `main`(commit e61006b),调度主链路端到端可跑
> 执行:直接在 `main` 分支进行(不开 worktree)

---

## 0. 定位

W3–4「可靠性」里程碑拆成两个子计划,本文档是**子计划一:可靠性核心**。
覆盖 spec W3–4 的前 5 项 + 消化 W1–2 backlog 的崩溃恢复缺口。
第二个子计划(Run Trace + 统一副作用 Policy 层)另起 spec,本文档不含。

**面试叙事**:削峰、幂等、死信、容错、水平扩展(国内后端八股核心)。

---

## 1. 范围

| # | 特性 | 一句话 |
|---|------|--------|
| ① | 崩溃恢复修复 | `ResultHandleService.handle` 加 `@Transactional`,消化 backlog 🔴 |
| ② | 幂等去重(Redisson + MD5) | worker 侧防重复"干活" |
| ③ | Kafka 自研阶梯延迟重试 + 死信 | 卖点5,对标 RocketMQ 延迟级别 |
| ④ | 超时兜底(@Scheduled 扫描) | 静默失败(worker 死/消息丢)的安全网 |
| ⑤ | 多 Worker + 优雅上下线 | 消费组 rebalance + 优雅停机 |

**不在本子计划**:Run Trace、统一副作用 Policy 层(子计划二);ZooKeeper 注册/选主(W9 可选);`1→3 Worker 吞吐比`等量化数字(W8 压测)。

---

## 2. 各特性设计

### ① 崩溃恢复修复(先做,地基)

**问题(backlog 🔴)**:`ResultHandleService.handle` 无事务,`子任务落定(CAS+updateById) → 计数 +1 → finalize` 三步分别提交。进程在"子任务已 COMPLETED、计数未 +1"之间崩溃 → Kafka 重投 → 子任务 CAS 已失败(幂等守卫 return)→ 计数永久漏计 → 任务卡 RUNNING 不终结。

**修复**:给 `handle` 加 `@Transactional`,三步原子提交。崩溃 → 干净回滚 → 重投时整段重做(子任务仍 DISPATCHED,可重新落定)。

**注意**:`handle` 内无 Kafka 发送(纯 DB 读写),事务边界干净;并发下每个 `handle` 各自原子提交,`RUNNING→terminal` 的 CAS 仍保证只有一个赢家聚合。

### ② 幂等去重(Redisson + MD5)—— worker 侧

- **幂等键** = `MD5(subtaskUuid + ":" + input)`,存 Redis,TTL **24h**。
- **消费前查**:键存在 = 这条已成功处理过(重复投递)→ 跳过处理,直接 ack,不重复跑 echo/LLM。
- **成功后置键**;**失败不置键**。
- **语义**:与 W1–2 server 侧子任务 CAS 互补——CAS 防重复"结果"落库,幂等键防重复"干活"(省 LLM 成本,对应必测指标3)。
- **用 Redisson**:`RBucket` + `setIfAbsent(key, ttl)` 原子置键;或分布式锁保护"查—处理—置键"临界区(二选一,实现期定,倾向 `setIfAbsent` 更轻)。

### ③ Kafka 自研阶梯延迟重试 + 死信(卖点5)

**为什么自研**:Kafka 无原生延迟消息/死信队列(RocketMQ 有)。自研 = 简历从"用了死信队列"升级为"在 Kafka 上实现了 RocketMQ 那套阶梯重试+死信语义,能对比讲两家 MQ 设计取舍"。

**topic 布局**:
```
AGENTFLOW_SUBTASK(主)  处理失败(attempt 从消息头读)
  ├ attempt=0→ RETRY_5S   消费者等到 (produceTime+5s) 再重投主 topic,attempt=1
  ├ attempt=1→ RETRY_30S  等 30s 重投,attempt=2
  ├ attempt=2→ RETRY_5M   等 5m 重投,attempt=3
  └ attempt≥3→ AGENTFLOW_DLQ(死信,不再自动重试)
```

- **延迟实现**:每个 RETRY topic 一个消费者;读到消息比较 `produceTime + level延迟` 与当前时间,**未到点则 pause 分区 + seek 回退 + sleep**(不 ack,到点再消费),到点重投主 topic。这是 Kafka 上"无原生延迟"的经典自研法。
- **attempt 计数**:放 Kafka 消息 header(`x-retry-attempt`),每跳 +1。与幂等键正交(幂等键防重复投递,attempt 管升级)。
- **恢复**:DLQ 独立消费者不自动重试;提供**手动/定时重放**接口把 DLQ 消息打回主 topic(attempt 归零)。归属明确("谁来恢复")。
- **档位可调**:5s / 30s / 5m、最多 3 次为默认,常量集中配置。

### ④ 超时兜底(server 侧 @Scheduled)

- 定时任务扫描 **子任务卡在 DISPATCHED 且 `updated_at` 早于阈值**(worker 死了/消息丢了,压根没回传)。
- 动作:**重投主 topic**(限次),超限标 FAILED → 触发任务终态判定。
- 与 ③ 区别:③ 是"worker 处理时抛异常"的重试;④ 是"根本没有任何结果回来"的静默失败兜底。触发源不同,都需要。
- 顺带兜住:崩溃后卡 RUNNING 的任务(①从源头防,④做最后防线,双保险)。
- 需要一列/机制记录 subtask 的 dispatch 时间与重投次数(复用 `updated_at` + 新增 `retry_count` 列或 Redis 计数,实现期定)。

### ⑤ 多 Worker + 优雅上下线

- **水平扩展**:Kafka 消费组 rebalance 天然支持;主 topic 已 3 分区(W1–2 设),最多 3 个 worker 并行。多实例=同 group 多进程,Kafka 自动分配分区。
- **优雅停机(自研重点)**:Spring Kafka 监听容器默认优雅关闭(处理完当前记录、提交 offset 再退)。显式加关闭钩子确认"处理完手头 subtask 再退",配合②幂等做到不丢不重。
- 多实例运行方式写进 README;吞吐提升比留 W8。

---

## 3. 关键交互(防互相打架)

- **幂等 × 重试**:幂等键仅成功置,合法重试(前次失败未置键)能重跑;`attempt` 在 header,与幂等键正交。
- **崩溃修复 × 超时**:`@Transactional` 从源头防撕裂;超时扫描兜住"事务都没跑成、worker 直接死"。双保险不重复。
- **重试 × 超时**:③处理异常→走 retry topic(有结果,只是失败);④静默失败→无任何回传,超时重投。两条路不重叠。

---

## 4. 任务顺序(每步可跑可测)

```
① 崩溃修复(@Transactional + 崩溃回滚测试)
② 幂等(Redisson setIfAbsent + 重复投递测试)
③ 重试/死信(RETRY_5S/30S/5M + DLQ + 恢复重放)
④ 超时扫描(@Scheduled + 卡死子任务重投)
⑤ 优雅停机(关闭钩子 + 多实例文档)
⑥ 端到端容错演练(kill worker / 注入失败,验证零丢失 + 死信 + 恢复)
```

---

## 5. 测试策略

- **单元**:幂等键生成/判定、attempt 升级规则、超时判定边界。
- **集成(真 MySQL + Kafka)**:
  - 失败消息走完 5S→30S→5M→DLQ 全链路(可缩短延迟档位加速测试)。
  - DLQ 重放回主 topic 成功消费。
  - 超时子任务被扫描重投。
  - `@Transactional`:模拟 increment 前抛异常 → 子任务状态回滚到 DISPATCHED。
  - 重复投递同一 subtask → 幂等键挡住,echo 只跑一次。
- **容错演练**:压测中 kill 一个 worker,在途任务零丢失(消息重投 + 幂等兜底)。

---

## 6. 与上游 spec 的对齐

- 落实 spec W3–4 行的:幂等、Kafka 自研阶梯重试/死信+恢复、超时兜底、多 Worker+优雅上下线。
- 落实 spec 卖点5「Kafka 可靠性层」。
- 消化 W1–2 backlog 🔴(崩溃恢复)。
- Run Trace / Policy 层 → 子计划二;ZK → W9;量化数字 → W8。

---

## 7. 风险与纪律

| 风险 | 兜底 |
|------|------|
| 范围膨胀(retry 机制易复杂化) | 严格 5s/30s/5m×3;不做优先级/多租户;新想法进 backlog |
| 延迟测试慢 | 测试用短档位(如 1s/2s/3s)加速,生产档位常量隔离 |
| pause/seek 自研延迟有坑 | 优先集成测试覆盖"未到点不消费/到点重投";坑记入报告 |
| 直接在 main 上做 | 每任务 TDD + 审查 + 频繁提交;可跑里程碑硬检查 |
