#!/usr/bin/env bash
# RESEARCH_AGENT 端到端:提交问题 → 轮询到终态 → 断言生成了报告。前置:docker compose up -d
set -euo pipefail
BASE="${BASE:-http://localhost:8080/api/v1/tasks}"
Q="${1:-What are the trade-offs between Kafka and RabbitMQ for agent systems?}"
# 等 server 就绪(compose --wait 只保证容器已启动,Spring 可能还在初始化)
for i in $(seq 1 60); do curl -sf "${BASE%/tasks}/llm/usage" >/dev/null && break; sleep 2; done

BODY=$(python3 -c 'import json,sys; print(json.dumps({"type":"RESEARCH_AGENT","payload":{"question":sys.argv[1]}}))' "$Q")
UUID=$(curl -sf -X POST "$BASE" -H 'Content-Type: application/json' -d "$BODY" \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["taskUuid"])')
echo "==> taskUuid=$UUID"

for i in $(seq 1 90); do
  TASK=$(curl -sf "$BASE/$UUID")
  STATUS=$(echo "$TASK" | python3 -c 'import sys,json; print(json.load(sys.stdin)["status"])')
  echo "    [$i] status=$STATUS"
  case "$STATUS" in
    COMPLETED|PARTIAL_FAILED)
      echo "$TASK" | python3 -c '
import sys,json
t=json.load(sys.stdin)
rep=[i["output"] for i in t["result"] if isinstance(i.get("output"),dict) and "report" in i["output"]]
assert rep, "没有生成报告"
print(rep[0]["report"]); print("来源数:", len(rep[0]["sources"]))
'
      echo "✅ E2E-AGENT PASS ($STATUS)"; exit 0;;
    FAILED) echo "$TASK" | python3 -m json.tool; echo "❌ 任务失败"; exit 1;;
  esac
  sleep 2
done
echo "❌ 180s 超时"; exit 1
