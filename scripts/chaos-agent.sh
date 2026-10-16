#!/usr/bin/env bash
# 容错演示:agent 跑到一半时杀掉它所在的 worker → 另一个 worker 从存档续跑,已调用的工具不重跑。
# 前置:docker compose up -d(两个 worker;mock 模式默认每次调用 1.5s,来得及杀)
set -euo pipefail
BASE="${BASE:-http://localhost:8080/api/v1/tasks}"
Q="${1:-How does Kafka consumer group rebalancing work?}"
# 等 server 就绪(compose --wait 只保证容器已启动,Spring 可能还在初始化)
for i in $(seq 1 60); do curl -sf "${BASE%/tasks}/llm/usage" >/dev/null && break; sleep 2; done

BODY=$(python3 -c 'import json,sys; print(json.dumps({"type":"RESEARCH_AGENT","payload":{"question":sys.argv[1]}}))' "$Q")
UUID=$(curl -sf -X POST "$BASE" -H 'Content-Type: application/json' -d "$BODY" \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["taskUuid"])')
echo "==> taskUuid=$UUID,等待第一个 agent 步骤…"

VICTIM=""
for i in $(seq 1 60); do
  VICTIM=$(curl -sf "$BASE/$UUID/trace" | python3 -c '
import sys,json
for e in json.load(sys.stdin):
    if e["stage"]=="AGENT_STEP":
        d=json.loads(e["detail"])
        if not d.get("final"):
            print(d["workerId"]); break
')
  [ -n "$VICTIM" ] && break
  sleep 1
done
[ -n "$VICTIM" ] || { echo "❌ 60s 内没有出现 agent 步骤"; exit 1; }

echo "==> docker kill agentflow-$VICTIM(它正在跑 agent 的下一步)"
docker kill "agentflow-$VICTIM" >/dev/null
trap 'docker start "agentflow-$VICTIM" >/dev/null' EXIT

echo "==> 等待其他 worker 接手(Kafka 会话超时约 45s)…"
STATUS=RUNNING
for i in $(seq 1 120); do
  STATUS=$(curl -sf "$BASE/$UUID" | python3 -c 'import sys,json; print(json.load(sys.stdin)["status"])')
  case "$STATUS" in COMPLETED|PARTIAL_FAILED|FAILED) break;; esac
  sleep 2
done

curl -sf "$BASE/$UUID/trace" | python3 -c '
import sys,json
res=[json.loads(e["detail"]) for e in json.load(sys.stdin) if e["stage"]=="RESUMED"]
assert res, "没有出现 RESUMED:agent 没有从存档续跑"
for r in res:
    print("  ⚡", r["workerId"], "从第", r["fromStep"] + 1, "步续跑")
'
echo "==> 任务终态:$STATUS"
if [ "$STATUS" = "FAILED" ]; then echo "❌ 任务失败"; exit 1; fi
echo "✅ CHAOS PASS"
