#!/usr/bin/env bash
# W4 Task 7:端到端验收——提交任务 → 轮询完成 → GET /trace 打印完整生命周期。
# 证明"闭环执行成功可审计":trace 应覆盖 SUBMITTED→...→TASK_FINALIZED。
set -euo pipefail
BASE="http://localhost:8080/api/v1/tasks"

UUID=$(curl -s -X POST "$BASE" -H 'Content-Type: application/json' \
  -d '{"type":"ECHO_BATCH","payload":{"items":["a","b","c"]}}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["taskUuid"])')
echo "taskUuid=$UUID"

for i in $(seq 1 30); do
  ST=$(curl -s "$BASE/$UUID" | python3 -c 'import sys,json;print(json.load(sys.stdin)["status"])')
  echo "  [poll $i] status=$ST"
  [ "$ST" = "COMPLETED" ] && break
  sleep 2
done

echo "==> Run Trace(闭环审计):"
curl -s "$BASE/$UUID/trace" | python3 -m json.tool
echo "✅ trace 覆盖 SUBMITTED→...→TASK_FINALIZED 即闭环执行成功可审计"
