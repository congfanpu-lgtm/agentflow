# Eval: v5-route

2026-11-10 21:34 · model `claude-haiku-5-5` · judge `claude-sonnet-5-5` · 20 questions

| Metric | Value |
|---|---|
| Completion rate | 100% (20/20) |
| Latency p50 / p95 | 27s / 40s |
| Cost per question (est.) | $0.0470 |
| Tokens / LLM calls per question | 46671.35 / 15.6 |
| Aspect coverage / depth (judge, 0-1-2 rubric) | 98% / 93% |
| Agents per question (incl. follow-ups) / review gaps | 3.9 / None |
| Citation accuracy strict / lenient (judge) | 62% / 87% (69 verifiable of 76 sampled) |
| Citation density (sentences with [n]) | 49% |
| Source utilization | 91% |
| Sources per report / agents with no sources | 8.85 / 6% |
| Report language matches question | 100% |
| Tool errors / citations dropped by guard | 19 / 0 |

| id | status | latency | depth | citations ok/checked | sources | gaps | lang |
|---|---|---|---|---|---|---|---|
| news-fomc | COMPLETED | 29.5s | 83% | 1/1 | 5 | — | ok |
| news-qqq | COMPLETED | 26.5s | 100% | 1/4 | 8 | — | ok |
| news-euai | COMPLETED | 44.7s | 83% | 3/4 | 15 | — | ok |
| news-nvda-zh | COMPLETED | 27.2s | 100% | 3/4 | 11 | — | ok |
| news-python | COMPLETED | 28.6s | 100% | 4/4 | 8 | — | ok |
| tech-kraft | COMPLETED | 39.6s | 100% | 3/4 | 12 | — | ok |
| tech-raft | COMPLETED | 27.5s | 100% | 3/3 | 5 | — | ok |
| tech-kafka-eos | COMPLETED | 22.5s | 100% | 3/4 | 7 | — | ok |
| tech-hnsw | COMPLETED | 27.3s | 67% | 2/4 | 5 | — | ok |
| tech-redis-zh | COMPLETED | 27.2s | 100% | 4/4 | 5 | — | ok |
| tech-mcp | COMPLETED | 33.5s | 100% | 3/3 | 9 | — | ok |
| cmp-kafka-rabbit | COMPLETED | 24.4s | 100% | 0/3 | 13 | — | ok |
| cmp-pgvector | COMPLETED | 27.4s | 100% | 1/4 | 14 | — | ok |
| cmp-grpc-zh | COMPLETED | 33.3s | 100% | 0/4 | 13 | — | ok |
| cmp-pg-mysql | COMPLETED | 27.2s | 83% | 2/3 | 14 | — | ok |
| cmp-agentfw-zh | COMPLETED | 21.3s | 83% | 0/0 | 3 | — | ok |
| ml-rag | COMPLETED | 24.3s | 83% | 3/4 | 8 | — | ok |
| ml-specdec | COMPLETED | 18.3s | 100% | 3/4 | 7 | — | ok |
| ml-lora-zh | COMPLETED | 27.3s | 83% | 1/4 | 5 | — | ok |
| ml-react | COMPLETED | 18.3s | 100% | 3/4 | 10 | — | ok |
