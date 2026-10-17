# Benchmarks & drills (2026-10-17)

Environment: Apple M5 (10 cores) · Docker Desktop · `docker compose` stack (MySQL, Redis, MongoDB, Kafka,
pgvector, RAG, server, **2 workers × 4 consumer threads**, 8 SUBTASK partitions). Numbers are machine-specific;
the ratios and pass/fail results are the point.

## 1. Scheduler throughput (no LLM)

`ECHO_BATCH` subtasks sleep 300 ms, so the ceiling is 8 threads ÷ 0.3 s = **26.7 subtasks/s**.

| Run | Tasks × subtasks | Concurrency | Submit P50 / P99 | End-to-end P50 / P99 | Throughput | Trace complete |
|---|---|---|---|---|---|---|
| `load_test.py --tasks 60 --items 5 --concurrency 10` | 60 × 5 | 10 | 44 / 579 ms | 1.9 / 2.9 s | 23.9 subtasks/s | 100% |
| `load_test.py --tasks 120 --items 5 --concurrency 20` | 120 × 5 | 20 | 36 / 357 ms | 3.8 / 4.3 s | **25.5 subtasks/s (96% of ceiling)** | 100% |

Reading it: Kafka dispatch, MySQL state CAS, idempotency checks, Mongo trace writes and polling
together add ~4% over pure work time at saturation; end-to-end latency grows with queue depth, not with
per-task overhead.

## 2. Worker killed under load

`TASKS=60 KILL_AFTER=3 scripts/chaos-load.sh` — `docker kill agentflow-worker-1` 3 s into the run, restart 30 s later.

- **60/60 tasks COMPLETED**, 300/300 subtasks processed (no loss, no duplicate processing).
- E2E P50 1.9 s, **P99 47.2 s**: the subtasks owned by the dead consumer wait for the Kafka session
  timeout (~45 s) before their partitions are reassigned. Lowering `session.timeout.ms` trades faster
  failover for more false rebalances.

## 3. Agent crash recovery (real model: Claude Haiku + Tavily)

`scripts/chaos-agent.sh "<question>"` waits for the first agent step, kills the worker running it, and
checks that another worker resumes from the Redis checkpoint.

| Question | Killed | Resumed | Result | Wall time |
|---|---|---|---|---|
| How does Kafka consumer group rebalancing work? | worker-2 | 2 agents on worker-1, from step 2 | COMPLETED | 71 s |
| What is the difference between Raft and Paxos? | worker-1 | 2 agents on worker-2, from step 2 | COMPLETED | 56 s |
| How does Redis Cluster shard keys? | worker-2 | 2 agents on worker-1, from step 2 | COMPLETED | 59 s |

3/3 recovered; completed steps (LLM decisions + tool results) were replayed from the checkpoint, not re-run.
Most of the wall time is the same ~45 s session timeout as above.

## 4. Research quality

See the evaluation table in the README and `eval/results/`.
