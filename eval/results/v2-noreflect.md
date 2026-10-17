# Eval: v2-noreflect

2026-10-16 22:49 · model `claude-haiku-5-5` · judge `claude-sonnet-5-5` · 20 questions

| Metric | Value |
|---|---|
| Completion rate | 100% (20/20) |
| Latency p50 / p95 | 21s / 30s |
| Cost per question (est.) | $0.0390 |
| Tokens / LLM calls per question | 38618.15 / 13.85 |
| Aspect coverage / depth (judge, 0-1-2 rubric) | 98% / 95% |
| Agents per question (incl. follow-ups) / review gaps | 4.0 / None |
| Citation accuracy strict / lenient (judge) | 54% / 86% (65 verifiable of 80 sampled) |
| Citation density (sentences with [n]) | 51% |
| Source utilization | 95% |
| Sources per report / agents with no sources | 6.95 / 25% |
| Report language matches question | 100% |
| Tool errors / citations dropped by guard | 5 / 0 |

| id | status | latency | depth | citations ok/checked | sources | gaps | lang |
|---|---|---|---|---|---|---|---|
| news-fomc | COMPLETED | 18.3s | 100% | 3/3 | 10 | — | ok |
| news-qqq | COMPLETED | 24.3s | 100% | 0/2 | 6 | — | ok |
| news-euai | COMPLETED | 30.4s | 100% | 1/2 | 7 | — | ok |
| news-nvda-zh | COMPLETED | 39.3s | 67% | 0/2 | 2 | — | ok |
| news-python | COMPLETED | 18.1s | 83% | 3/4 | 10 | — | ok |
| tech-kraft | COMPLETED | 24.2s | 100% | 2/3 | 8 | — | ok |
| tech-raft | COMPLETED | 18.1s | 100% | 2/4 | 7 | — | ok |
| tech-kafka-eos | COMPLETED | 24.2s | 100% | 3/4 | 7 | — | ok |
| tech-hnsw | COMPLETED | 21.2s | 67% | 1/2 | 7 | — | ok |
| tech-redis-zh | COMPLETED | 18.1s | 100% | 3/4 | 6 | — | ok |
| tech-mcp | COMPLETED | 21.2s | 100% | 4/4 | 10 | — | ok |
| cmp-kafka-rabbit | COMPLETED | 21.1s | 100% | 1/4 | 5 | — | ok |
| cmp-pgvector | COMPLETED | 24.2s | 100% | 2/3 | 2 | — | ok |
| cmp-grpc-zh | COMPLETED | 24.1s | 100% | 1/4 | 8 | — | ok |
| cmp-pg-mysql | COMPLETED | 27.2s | 100% | 1/4 | 4 | — | ok |
| cmp-agentfw-zh | COMPLETED | 18.1s | 100% | 0/1 | 8 | — | ok |
| ml-rag | COMPLETED | 21.1s | 83% | 3/4 | 7 | — | ok |
| ml-specdec | COMPLETED | 15.1s | 100% | 0/4 | 6 | — | ok |
| ml-lora-zh | COMPLETED | 15.1s | 100% | 3/4 | 7 | — | ok |
| ml-react | COMPLETED | 18.1s | 100% | 2/3 | 12 | — | ok |
