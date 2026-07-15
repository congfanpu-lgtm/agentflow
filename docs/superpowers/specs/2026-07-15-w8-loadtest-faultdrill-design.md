# W8 压测 + 容错演练 · 设计文档(子计划五)

> 创建:2026-07-15 · 状态:定稿
> 上游:`docs/specs/2026-07-11-agentflow-design.md` 第 4 节「必测指标(7 项)」
> 前置:W1–7 已在 `main`(commit cea788a),Java 85 测试 + Python 6 测试绿
> 执行:直接在 `main` 分支

---

## 0. 定位

W8 里程碑:**全部量化数字**。不新增业务功能,而是把前面各里程碑的能力**实测成简历可写的数字**,
并保留脚本可复现(项目原则:所有简历数字来自实测)。对应 master 第 4 节 7 项必测指标。

**测量哲学**:压测用 **mock LLM + 确定性 embedding**(master 第 149 行"压测用 mock LLM 测调度不烧钱")——
被测的是**调度/并发/容错/幂等/可观测**这些自研核心,不是 LLM 本身。数字与机器相关,如实标注环境。

**压测工具选择(自主决策)**:本机无 k6/JMeter/hey,只有 `ab`(不适合"提交→轮询"的异步任务模型)。
用 **Python 标准库**(`concurrent.futures` + `urllib`)自写并发压测脚本——零新依赖、跨机可跑、
逻辑透明可讲。master 写"k6 或 JMeter"是举例,自写 harness 同样产出 P50/P99/吞吐,且更贴合本项目
"提交任务→异步完成"的度量语义(要测端到端完成延迟,不只是 HTTP 响应延迟)。

---

## 1. 七项指标与测量方法

| # | 指标 | 测量方法 | 数据源 |
|---|------|---------|--------|
| 1 | **吞吐 / 延迟** | 并发提交 N 个任务(每任务 M 子任务),测提交延迟 + **端到端完成延迟**(提交→COMPLETED)P50/P99,及 任务/分钟、子任务/秒 | `load_test.py` 计时 |
| 2 | **水平扩展** | 同一负载分别在 **1 / 3 worker** 下跑,对比子任务吞吐提升比 | `load_test.py` × 两次 worker 配置 |
| 3 | **成本控制(幂等拦截)** | 容错演练中 kill worker 触发重投,统计 trace 里 `PROCESSED status=SKIPPED_IDEMPOTENT` 占比 = 被幂等拦截、**没有重复调 LLM** 的比例 | trace 事件计数 |
| 4 | **容错(零丢失)** | 压测中途 kill 一个 worker,验证**所有任务最终 COMPLETED**、子任务无永久丢失 | `fault_drill` + 状态核对 |
| 5 | **检索质量 Recall@K** | 已于 W7 实测(mean Recall@3=1.0,确定性 embedding) | `agentflow-rag/tests/test_recall.py` |
| 6 | **副作用幂等(零重复)** | 同一任务二次 finalize,通知只发一次;已有 `NotificationDedupeTest` 证明,压测中核对通知日志计数 = 任务数 | 既有测试 + 日志计数 |
| 7 | **可观测 / trace 完整率** | 对压测产生的每个任务,校验 trace 覆盖 `SUBMITTED→DECOMPOSED→…→TASK_FINALIZED`,算完整任务占比 | trace API 逐任务校验 |

---

## 2. 压测脚本设计(`scripts/load_test.py`)

- 纯标准库(`concurrent.futures.ThreadPoolExecutor` + `urllib.request` + `time`)。
- 参数:`--tasks N`(任务数)、`--items M`(每任务子任务数)、`--type`(ECHO_BATCH 默认,压测调度不烧钱)、
  `--concurrency C`(并发提交/轮询线程)、`--base`(API)。
- 流程:并发提交 N 任务(记提交延迟)→ 并发轮询各任务到 COMPLETED(记端到端延迟)→ 汇总:
  - 提交延迟 P50/P99;端到端完成延迟 P50/P99;
  - 墙钟总耗时、任务/分钟、子任务/秒(= N×M / 总耗时);
  - 失败任务数(非 COMPLETED)。
- 输出 JSON + 人读表格,便于贴报告 + 脚本可复现。

## 3. 指标汇总脚本(`scripts/metrics.sh`)

- 调 `load_test.py` 跑一轮 → 取返回的 taskUuid 列表 → 逐个查 `/trace` 算**trace 完整率**(指标7)与
  **幂等拦截数**(指标3,数 SKIPPED_IDEMPOTENT)。
- 打印 7 项指标汇总(5/6 引用既有结果 + 日志核对)。

## 4. 容错演练(指标 4 + 3)

- 起 server + 2 worker;`load_test.py` 提交一批任务;演练中 `kill -9` 一个 worker;
- 等全部任务收敛,核对:全部 COMPLETED(零丢失,指标4);trace 中 SKIPPED_IDEMPOTENT > 0
  (kill 后在途消息重投、已处理的被幂等拦截,指标3)。
- 复用/增强既有 `fault-drill.sh`。

## 5. 水平扩展(指标 2)

- 同一 `load_test.py` 负载:先 1 worker 跑记子任务/秒,再 3 worker 跑记子任务/秒,算提升比。
- 主 topic 3 分区(已配),故 3 worker 可并行吃 3 分区。记录实测比(理想≤3×,受单机 CPU/DB 限制)。

## 6. 任务顺序
```
T1 load_test.py(stdlib 并发压测:P50/P99/吞吐)+ metrics.sh(trace 完整率/幂等拦截)
T2 实盘:1 worker 基线 → 3 worker 扩展;容错演练(kill worker);汇总 7 项真实数字
T3 报告 + README/backlog(数字 + 环境标注 + 复现命令)
```

## 7. 风险与纪律
| 风险 | 兜底 |
|---|---|
| 数字机器相关不可比 | 报告标注环境(CPU/JVM/容器);给复现命令;强调相对值(扩展比)而非绝对值 |
| 压测烧钱 | 全程 mock LLM(ECHO_BATCH / LLM_BATCH mock),不调真模型 |
| 单机 3 worker 抢 CPU 使扩展比 <3× | 如实记录 + 解释(单机瓶颈非架构瓶颈),这是诚实的工程判断 |
| 幂等拦截难稳定复现 | 用 kill -9 制造在途中断 + earliest offset 重投,trace 计数为准 |
| 不引 k6/JMeter | 自写 stdlib harness;测端到端完成延迟(比纯 HTTP 压测更贴任务语义) |
