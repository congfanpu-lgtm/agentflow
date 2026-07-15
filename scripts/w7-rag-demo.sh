#!/usr/bin/env bash
# W7 Task 5:端到端验收——灌库 → 提交 RESEARCH_BATCH(一条需检索、一条不需)→ 打印结果。
# 证明:意图驱动检索(命中才查)+ 多角色(检索→汇总)+ 跨语言(Java 调 Python RAG 服务)。
# 前置:pgvector(5433)、RAG 服务(8000)、server(8080)、1 worker 均在跑。
set -euo pipefail
API="http://localhost:8080/api/v1"
RAG="http://localhost:8000"

echo "==> 灌库(几条 doc):"
curl -s -X POST "$RAG/rag/ingest" -H 'Content-Type: application/json' -d '{
  "docs":[
    {"id":"kafka-2","text":"Kafka consumer group rebalance reassigns partitions when workers join or leave"},
    {"id":"redis-1","text":"Redis token bucket rate limiting uses a Lua script for atomic operations"},
    {"id":"pgvector-1","text":"pgvector stores embeddings and does approximate nearest neighbor cosine search"}
  ]}' | python3 -m json.tool

echo "==> 提交 RESEARCH_BATCH(q1 需检索,q2 不需):"
UUID=$(curl -s -X POST "$API/tasks" -H 'Content-Type: application/json' -d '{
  "type":"RESEARCH_BATCH",
  "payload":{"questions":["kafka 如何在 worker 离开时重分配分区?","hello there"],"model":"mock-small"}
}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["taskUuid"])')
echo "taskUuid=$UUID"

for i in $(seq 1 30); do
  ST=$(curl -s "$API/tasks/$UUID" | python3 -c 'import sys,json;print(json.load(sys.stdin)["status"])')
  [ "$ST" = "COMPLETED" ] && break; sleep 2
done

echo "==> 结果(看 usedRag / retrieved):"
curl -s "$API/tasks/$UUID" | python3 -m json.tool
echo "==> Run Trace:"; curl -s "$API/tasks/$UUID/trace" | python3 -c 'import sys,json;[print(e["stage"],e.get("status")) for e in json.load(sys.stdin)]'
echo "==> LLM usage:"; curl -s "$API/llm/usage" | python3 -m json.tool
echo "✅ 需检索的 q1 usedRag=true 且 retrieved 含 kafka-2;不需的 q2 usedRag=false 不检索"
