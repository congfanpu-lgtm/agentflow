# Eval: v3-evidence

2026-10-16 23:01 · model `claude-haiku-5-5` · judge `claude-sonnet-5-5` · 20 questions

| Metric | Value |
|---|---|
| Completion rate | 100% (20/20) |
| Latency p50 / p95 | 21s / 33s |
| Cost per question (est.) | $0.0460 |
| Tokens / LLM calls per question | 45516.05 / 14.4 |
| Aspect coverage / depth (judge, 0-1-2 rubric) | 98% / 91% |
| Agents per question (incl. follow-ups) / review gaps | 3.85 / None |
| Citation accuracy strict / lenient (judge) | 65% / 85% (40 verifiable of 40 sampled) |
| Citation density (sentences with [n]) | 15% |
| Source utilization | 64% |
| Sources per report / agents with no sources | 2.0 / 69% |
| Report language matches question | 100% |
| Tool errors / citations dropped by guard | 9 / 0 |

| id | status | latency | depth | citations ok/checked | sources | gaps | lang |
|---|---|---|---|---|---|---|---|
| news-fomc | COMPLETED | 21.3s | 83% | 1/4 | 5 | — | ok |
| news-qqq | COMPLETED | 39.4s | 100% | 1/1 | 1 | — | ok |
| news-euai | COMPLETED | 21.3s | 83% | 3/4 | 2 | — | ok |
| news-nvda-zh | COMPLETED | 27.2s | 100% | 0/0 | 0 | — | ok |
| news-python | COMPLETED | 18.1s | 83% | 0/4 | 8 | — | ok |
| tech-kraft | COMPLETED | 30.2s | 83% | 0/0 | 0 | — | ok |
| tech-raft | COMPLETED | 18.2s | 100% | 0/0 | 0 | — | ok |
| tech-kafka-eos | COMPLETED | 24.1s | 100% | 4/4 | 2 | — | ok |
| tech-hnsw | COMPLETED | 21.1s | 67% | 2/4 | 2 | — | ok |
| tech-redis-zh | COMPLETED | 21.2s | 100% | 0/1 | 2 | — | ok |
| tech-mcp | COMPLETED | 12.1s | 83% | 3/4 | 5 | — | ok |
| cmp-kafka-rabbit | COMPLETED | 24.1s | 100% | 1/2 | 2 | — | ok |
| cmp-pgvector | COMPLETED | 33.2s | 67% | 0/0 | 0 | — | ok |
| cmp-grpc-zh | COMPLETED | 27.3s | 100% | 0/0 | 0 | — | ok |
| cmp-pg-mysql | COMPLETED | 27.2s | 100% | 0/0 | 0 | — | ok |
| cmp-agentfw-zh | COMPLETED | 18.2s | 83% | 0/0 | 0 | — | ok |
| ml-rag | COMPLETED | 27.2s | 83% | 2/2 | 2 | — | ok |
| ml-specdec | COMPLETED | 15.2s | 100% | 4/4 | 4 | — | ok |
| ml-lora-zh | COMPLETED | 24.1s | 100% | 2/2 | 1 | — | ok |
| ml-react | COMPLETED | 18.2s | 100% | 3/4 | 4 | — | ok |
