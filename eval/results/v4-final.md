# Eval: v4-final

2026-10-16 23:27 · model `claude-haiku-5-5` · judge `claude-sonnet-5-5` · 20 questions

| Metric | Value |
|---|---|
| Completion rate | 100% (20/20) |
| Latency p50 / p95 | 24s / 36s |
| Cost per question (est.) | $0.0490 |
| Tokens / LLM calls per question | 48648.25 / 15.0 |
| Aspect coverage / depth (judge, 0-1-2 rubric) | 100% / 95% |
| Agents per question (incl. follow-ups) / review gaps | 4.0 / None |
| Citation accuracy strict / lenient (judge) | 60% / 87% (68 verifiable of 76 sampled) |
| Citation density (sentences with [n]) | 46% |
| Source utilization | 92% |
| Sources per report / agents with no sources | 7.8 / 12% |
| Report language matches question | 100% |
| Tool errors / citations dropped by guard | 3 / 0 |

| id | status | latency | depth | citations ok/checked | sources | gaps | lang |
|---|---|---|---|---|---|---|---|
| news-fomc | COMPLETED | 24.5s | 100% | 3/4 | 7 | — | ok |
| news-qqq | COMPLETED | 24.5s | 100% | 1/3 | 11 | — | ok |
| news-euai | COMPLETED | 36.6s | 83% | 2/4 | 14 | — | ok |
| news-nvda-zh | COMPLETED | 27.2s | 100% | 0/0 | 12 | — | ok |
| news-python | COMPLETED | 36.3s | 100% | 1/4 | 8 | — | ok |
| tech-kraft | COMPLETED | 24.2s | 100% | 3/4 | 7 | — | ok |
| tech-raft | COMPLETED | 18.2s | 100% | 0/0 | 6 | — | ok |
| tech-kafka-eos | COMPLETED | 27.2s | 100% | 2/2 | 7 | — | ok |
| tech-hnsw | COMPLETED | 21.2s | 100% | 2/4 | 6 | — | ok |
| tech-redis-zh | COMPLETED | 30.2s | 100% | 2/4 | 6 | — | ok |
| tech-mcp | COMPLETED | 18.1s | 83% | 2/4 | 6 | — | ok |
| cmp-kafka-rabbit | COMPLETED | 30.2s | 83% | 2/4 | 14 | — | ok |
| cmp-pgvector | COMPLETED | 27.2s | 100% | 4/4 | 10 | — | ok |
| cmp-grpc-zh | COMPLETED | 21.2s | 100% | 0/4 | 6 | — | ok |
| cmp-pg-mysql | COMPLETED | 24.2s | 83% | 2/4 | 8 | — | ok |
| cmp-agentfw-zh | COMPLETED | 18.1s | 100% | 3/4 | 4 | — | ok |
| ml-rag | COMPLETED | 21.1s | 83% | 2/3 | 10 | — | ok |
| ml-specdec | COMPLETED | 15.1s | 83% | 3/4 | 4 | — | ok |
| ml-lora-zh | COMPLETED | 21.2s | 100% | 3/4 | 4 | — | ok |
| ml-react | COMPLETED | 18.1s | 100% | 4/4 | 6 | — | ok |
