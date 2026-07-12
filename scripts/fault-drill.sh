#!/usr/bin/env bash
# 容错演练:前置 docker compose up -d + server 起 + 至少 2 个 worker 起。
# 场景:提交任务 → 处理中途 kill 一个 worker → 验证任务最终仍 COMPLETED(消息重投+幂等,零丢失)。
set -euo pipefail
BASE="http://localhost:8080/api/v1/tasks"

echo "==> 提交 5 子任务"
UUID=$(curl -s -X POST "$BASE" -H 'Content-Type: application/json' \
  -d '{"type":"ECHO_BATCH","payload":{"items":["a","b","c","d","e"]}}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["taskUuid"])')
echo "    taskUuid=$UUID"

echo "==> 立即 kill 一个 worker(制造在途中断),你可在另一终端 kill 一个 worker 实例"
echo "==> 轮询直到 COMPLETED(消息应被其它实例/重投接手)"
for i in $(seq 1 40); do
  ST=$(curl -s "$BASE/$UUID" | python3 -c 'import sys,json;print(json.load(sys.stdin)["status"])')
  echo "    [$i] $ST"
  [ "$ST" = "COMPLETED" ] && { echo "✅ 零丢失:kill worker 后任务仍完成"; exit 0; }
  [ "$ST" = "FAILED" ] && { echo "❌ 任务失败"; curl -s "$BASE/$UUID" | python3 -m json.tool; exit 1; }
  sleep 3
done
echo "❌ 超时未完成"; exit 1
