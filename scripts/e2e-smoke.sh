#!/usr/bin/env bash
# 端到端冒烟:前置要求 docker compose up -d 且 server(8080)、worker 均已启动。
set -euo pipefail

BASE="http://localhost:8080/api/v1/tasks"

echo "==> 提交 ECHO_BATCH 任务(3 个子任务)"
UUID=$(curl -s -X POST "$BASE" -H 'Content-Type: application/json' \
  -d '{"type":"ECHO_BATCH","payload":{"items":["hello","agent","flow"]}}' \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["taskUuid"])')
echo "    taskUuid=$UUID"

echo "==> 轮询任务状态(最多 60s)"
for i in $(seq 1 30); do
  STATUS=$(curl -s "$BASE/$UUID" | python3 -c 'import sys,json; print(json.load(sys.stdin)["status"])')
  echo "    [$i] status=$STATUS"
  if [ "$STATUS" = "COMPLETED" ]; then
    echo "==> 最终结果:"
    curl -s "$BASE/$UUID" | python3 -m json.tool
    echo "✅ E2E PASS:调度主链路贯通"
    exit 0
  fi
  if [ "$STATUS" = "FAILED" ]; then
    echo "❌ E2E FAIL:任务失败"
    curl -s "$BASE/$UUID" | python3 -m json.tool
    exit 1
  fi
  sleep 2
done
echo "❌ E2E FAIL:60s 超时未完成"
exit 1
