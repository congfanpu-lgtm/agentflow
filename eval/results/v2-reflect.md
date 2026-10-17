# Eval: v2-reflect

2026-10-16 22:55 · model `claude-haiku-5-5` · judge `claude-sonnet-5-5` · 20 questions

| Metric | Value |
|---|---|
| Completion rate | 100% (20/20) |
| Latency p50 / p95 | 39s / 58s |
| Cost per question (est.) | $0.0720 |
| Tokens / LLM calls per question | 72476.45 / 21.95 |
| Aspect coverage / depth (judge, 0-1-2 rubric) | 100% / 95% |
| Agents per question (incl. follow-ups) / review gaps | 5.95 / 2.0 |
| Citation accuracy strict / lenient (judge) | 47% / 82% (74 verifiable of 80 sampled) |
| Citation density (sentences with [n]) | 45% |
| Source utilization | 85% |
| Sources per report / agents with no sources | 7.6 / 40% |
| Report language matches question | 100% |
| Tool errors / citations dropped by guard | 12 / 4 |

| id | status | latency | depth | citations ok/checked | sources | gaps | lang |
|---|---|---|---|---|---|---|---|
| news-fomc | COMPLETED | 36.3s | 100% | 2/3 | 14 | 2 | ok |
| news-qqq | COMPLETED | 57.5s | 83% | 1/3 | 3 | 2 | ok |
| news-euai | COMPLETED | 51.4s | 83% | 3/4 | 8 | 2 | ok |
| news-nvda-zh | COMPLETED | 57.4s | 100% | 3/4 | 10 | 2 | ok |
| news-python | COMPLETED | 30.2s | 83% | 3/4 | 12 | 2 | ok |
| tech-kraft | COMPLETED | 60.4s | 100% | 2/4 | 9 | 2 | ok |
| tech-raft | COMPLETED | 30.2s | 100% | 2/2 | 6 | 2 | ok |
| tech-kafka-eos | COMPLETED | 46.4s | 83% | 4/4 | 7 | 2 | ok |
| tech-hnsw | COMPLETED | 51.5s | 100% | 1/4 | 7 | 2 | ok |
| tech-redis-zh | COMPLETED | 33.3s | 100% | 3/4 | 7 | 2 | ok |
| tech-mcp | COMPLETED | 36.3s | 100% | 2/4 | 12 | 2 | ok |
| cmp-kafka-rabbit | COMPLETED | 42.3s | 83% | 1/4 | 4 | 2 | ok |
| cmp-pgvector | COMPLETED | 42.3s | 83% | 1/4 | 7 | 2 | ok |
| cmp-grpc-zh | COMPLETED | 39.2s | 100% | 0/4 | 7 | 2 | ok |
| cmp-pg-mysql | COMPLETED | 45.4s | 100% | 0/4 | 7 | 2 | ok |
| cmp-agentfw-zh | COMPLETED | 30.2s | 100% | 0/2 | 5 | 2 | ok |
| ml-rag | COMPLETED | 39.3s | 100% | 2/4 | 6 | 2 | ok |
| ml-specdec | COMPLETED | 36.2s | 100% | 2/4 | 6 | 2 | ok |
| ml-lora-zh | COMPLETED | 33.3s | 100% | 1/4 | 10 | 2 | ok |
| ml-react | COMPLETED | 42.4s | 100% | 2/4 | 5 | 2 | ok |
