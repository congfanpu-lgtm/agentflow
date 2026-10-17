# Eval: v3-evidence-1600tok

2026-10-16 23:07 · model `claude-haiku-5-5` · judge `claude-sonnet-5-5` · 20 questions

| Metric | Value |
|---|---|
| Completion rate | 100% (20/20) |
| Latency p50 / p95 | 21s / 36s |
| Cost per question (est.) | $0.0470 |
| Tokens / LLM calls per question | 47225.05 / 14.5 |
| Aspect coverage / depth (judge, 0-1-2 rubric) | 100% / 93% |
| Agents per question (incl. follow-ups) / review gaps | 3.95 / None |
| Citation accuracy strict / lenient (judge) | 67% / 85% (67 verifiable of 73 sampled) |
| Citation density (sentences with [n]) | 42% |
| Source utilization | 91% |
| Sources per report / agents with no sources | 8.1 / 13% |
| Report language matches question | 100% |
| Tool errors / citations dropped by guard | 8 / 0 |

| id | status | latency | depth | citations ok/checked | sources | gaps | lang |
|---|---|---|---|---|---|---|---|
| news-fomc | COMPLETED | 27.5s | 100% | 2/4 | 10 | — | ok |
| news-qqq | COMPLETED | 45.6s | 100% | 3/3 | 8 | — | ok |
| news-euai | COMPLETED | 36.5s | 83% | 3/3 | 12 | — | ok |
| news-nvda-zh | COMPLETED | 24.2s | 100% | 0/4 | 13 | — | ok |
| news-python | COMPLETED | 21.2s | 100% | 1/4 | 6 | — | ok |
| tech-kraft | COMPLETED | 24.1s | 100% | 4/4 | 10 | — | ok |
| tech-raft | COMPLETED | 21.2s | 100% | 2/3 | 8 | — | ok |
| tech-kafka-eos | COMPLETED | 21.2s | 100% | 2/4 | 6 | — | ok |
| tech-hnsw | COMPLETED | 21.2s | 100% | 2/4 | 6 | — | ok |
| tech-redis-zh | COMPLETED | 18.1s | 100% | 0/0 | 4 | — | ok |
| tech-mcp | COMPLETED | 18.2s | 100% | 4/4 | 9 | — | ok |
| cmp-kafka-rabbit | COMPLETED | 30.2s | 100% | 2/4 | 13 | — | ok |
| cmp-pgvector | COMPLETED | 27.2s | 67% | 4/4 | 10 | — | ok |
| cmp-grpc-zh | COMPLETED | 24.2s | 100% | 0/2 | 9 | — | ok |
| cmp-pg-mysql | COMPLETED | 27.2s | 67% | 3/4 | 6 | — | ok |
| cmp-agentfw-zh | COMPLETED | 21.2s | 83% | 0/1 | 2 | — | ok |
| ml-rag | COMPLETED | 33.3s | 83% | 2/4 | 9 | — | ok |
| ml-specdec | COMPLETED | 18.1s | 100% | 4/4 | 5 | — | ok |
| ml-lora-zh | COMPLETED | 21.2s | 100% | 3/3 | 8 | — | ok |
| ml-react | COMPLETED | 18.2s | 83% | 4/4 | 8 | — | ok |
