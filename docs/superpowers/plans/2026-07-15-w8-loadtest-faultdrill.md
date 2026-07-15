# W8 压测 + 容错演练 Implementation Plan(子计划五)

> **For agentic workers:** Steps 用 checkbox。W8 是"测量+记录"里程碑,几乎无业务代码,重在脚本 + 实盘数字。

**Goal:** 用零依赖 Python 压测脚本实测 master 第 4 节 7 项指标,保留脚本可复现,数字写进报告。

**Architecture:** 见 `docs/superpowers/specs/2026-07-15-w8-loadtest-faultdrill-design.md`。

**Tech Stack:** Python 3 标准库(concurrent.futures + urllib);既有 Java 栈(mock LLM 压测)。

## Global Constraints
- **直接在 `main` 分支**;当前 `main` @ `cea788a`,Java 85 + Python 6 测试绿。
- **压测全程 mock LLM / 确定性 embedding**,不烧钱。
- 脚本**零新依赖**(标准库);数字**标注环境**,给复现命令。
- 构建/起服务用 `JAVA_HOME=$(/usr/libexec/java_home -v 21)`;server `-f agentflow-server/pom.xml spring-boot:run`,worker 同理(可起多个)。

---

### Task 1: 压测脚本 + 指标汇总

**Files:** `scripts/load_test.py`、`scripts/metrics.sh`

- [ ] **Step 1:** `load_test.py`(stdlib):`--tasks/--items/--type/--concurrency/--base`;并发提交(记提交延迟)→ 并发轮询到 COMPLETED(记端到端延迟)→ 输出 P50/P99、任务/分钟、子任务/秒、失败数、taskUuids(JSON)。
- [ ] **Step 2:** `metrics.sh`:跑 `load_test.py` → 对每个 taskUuid 查 `/trace` → 算 trace 完整率(含 SUBMITTED 且含 TASK_FINALIZED 的任务占比)+ 幂等拦截数(SKIPPED_IDEMPOTENT 计数)。打印 7 项汇总(5/6 引用既有)。
- [ ] **Step 3:** `load_test.py` 自带一个 `--self-check`(对拍:小样本 assert 统计函数正确,如 P99 计算),或独立 `assert` demo。commit `feat: W8 压测脚本(stdlib 并发,P50/P99/吞吐)+ 指标汇总`。

---

### Task 2: 实盘跑指标 + 容错演练

**前置:** docker(MySQL/Kafka/Redis/Mongo/pgvector)、server、worker 起(mock LLM)。

- [ ] **Step 1:** 起 server + 1 worker → `load_test.py --tasks 30 --items 5`(150 子任务)→ 记基线:提交/端到端 P50/P99、子任务/秒。
- [ ] **Step 2:** 再起到 3 worker(同消费组)→ 同负载再跑 → 记子任务/秒;算 1→3 扩展比。
- [ ] **Step 3:** 容错演练:3 worker 下提交一批 → `kill -9` 一个 worker → 等收敛 → 核对全部 COMPLETED(零丢失)+ trace SKIPPED_IDEMPOTENT 计数(幂等拦截)。
- [ ] **Step 4:** `metrics.sh` 跑一轮取 trace 完整率;核对通知去重(日志 `📣` 计数 == 任务数,`副作用去重跳过` 为二次触发)。
- [ ] **Step 5:** 汇总 7 项真实数字(含环境)。

---

### Task 3: 报告 + 文档收尾

**Files:** `.superpowers/sdd/progress-w8.md`(本地)、`README.md`、`docs/backlog.md`

- [ ] **Step 1:** 进度报告:7 项指标真实数字 + 环境标注 + 复现命令。
- [ ] **Step 2:** README 加 "## 压测与指标(W8)" 表格(数字 + 复现)+ 路线图勾选。backlog 记 W8 交付 + 自检 + 发现。
- [ ] **Step 3:** 里程碑自检:① 为何测端到端完成延迟而非 HTTP 延迟?② 1→3 扩展比 <3× 说明什么?③ 幂等拦截率怎么算、证明省了什么?④ 压测为何用 mock LLM?
- [ ] **Step 4:** commit `feat: W8 压测与容错演练实测 7 项指标 + 里程碑验收`。

---

## Self-Review
- **Spec 覆盖**:脚本(指标1/7/3)=T1;实盘(指标1/2/3/4)+核对(6)=T2;报告(引用5/6)=T3。7 项全覆盖。
- **需实现者决断**:(a) 自写 stdlib 压测替代 k6(测端到端语义、零依赖);(b) 幂等拦截用 trace SKIPPED 计数;(c) 数字标注环境、强调扩展比等相对值。均已在任务内明确。
- **风险**:数字机器相关——报告标注环境 + 复现命令;单机 3 worker 扩展比可能 <3×,如实记录并解释。
