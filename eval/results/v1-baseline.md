# Eval: v1-baseline (binary coverage rubric, before the Wikipedia fix)

2026-10-16 22:39 · model `claude-haiku-5-5` · judge `claude-sonnet-5-5` · 20 questions

| Metric | Value |
|---|---|
| Completion rate | 100% (20/20) |
| Latency p50 / p95 | 21s / 36s |
| Cost per question (est.) | $0.0380 |
| Tokens / LLM calls per question | 38253.3 / 14.1 |
| Aspect coverage (judge) | 100% |
| Citation accuracy strict / lenient (judge) | 56% / 75% (64 verifiable of 76 sampled) |
| Citation density (sentences with [n]) | 49% |
| Source utilization | 90% |
| Sources per report | 6.1 |
| Report language matches question | 100% |
| Tool errors / citations dropped by guard | 41 / 2 |

| id | status | latency | coverage | citations ok/checked | sources | lang |
|---|---|---|---|---|---|---|
| news-fomc | COMPLETED | 21.4s | 100% | 0/0 | 7 | ok |
| news-qqq | COMPLETED | 36.5s | 100% | 1/2 | 6 | ok |
| news-euai | COMPLETED | 21.4s | 100% | 2/4 | 7 | ok |
| news-nvda-zh | COMPLETED | 30.2s | 100% | 2/4 | 4 | ok |
| news-python | COMPLETED | 24.3s | 100% | 3/3 | 8 | ok |
| tech-kraft | COMPLETED | 18.1s | 100% | 1/3 | 10 | ok |
| tech-raft | COMPLETED | 21.2s | 100% | 3/4 | 7 | ok |
| tech-kafka-eos | COMPLETED | 21.1s | 100% | 3/4 | 4 | ok |
| tech-hnsw | COMPLETED | 21.2s | 100% | 1/4 | 8 | ok |
| tech-redis-zh | COMPLETED | 21.2s | 100% | 1/4 | 6 | ok |
| tech-mcp | COMPLETED | 21.2s | 100% | 2/4 | 8 | ok |
| cmp-kafka-rabbit | COMPLETED | 30.2s | 100% | 0/0 | 0 | ok |
| cmp-pgvector | COMPLETED | 24.1s | 100% | 2/4 | 3 | ok |
| cmp-grpc-zh | COMPLETED | 24.2s | 100% | 1/4 | 9 | ok |
| cmp-pg-mysql | COMPLETED | 30.2s | 100% | 3/4 | 4 | ok |
| cmp-agentfw-zh | COMPLETED | 18.1s | 100% | 2/2 | 7 | ok |
| ml-rag | COMPLETED | 24.1s | 100% | 2/4 | 8 | ok |
| ml-specdec | COMPLETED | 15.1s | 100% | 2/4 | 4 | ok |
| ml-lora-zh | COMPLETED | 84.7s | 100% | 2/2 | 6 | ok |
| ml-react | COMPLETED | 18.1s | 100% | 3/4 | 6 | ok |
