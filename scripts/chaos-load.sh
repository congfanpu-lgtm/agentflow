#!/usr/bin/env bash
# 压测中途杀 worker:后台跑 load_test(ECHO_BATCH,不调模型),KILL_AFTER 秒后 docker kill 一个 worker,
# 30s 后再拉起;最后断言所有任务都 COMPLETED(Kafka 重投 + 幂等兜底 + 超时重扫,零丢失)。
# 前置:docker compose up -d(2 个 worker)。
set -euo pipefail
cd "$(dirname "$0")/.."
TASKS="${TASKS:-40}"; ITEMS="${ITEMS:-5}"; KILL_AFTER="${KILL_AFTER:-4}"; VICTIM="${VICTIM:-agentflow-worker-1}"
OUT="$(mktemp)"

python3 scripts/load_test.py --tasks "$TASKS" --items "$ITEMS" --concurrency 10 --deadline 300 --trace-check > "$OUT" &
LT=$!
trap 'docker start "$VICTIM" >/dev/null 2>&1 || true' EXIT
sleep "$KILL_AFTER"
echo "==> $(date +%T) docker kill $VICTIM(压测进行中)"
docker kill "$VICTIM" >/dev/null
sleep 30
echo "==> $(date +%T) docker start $VICTIM"
docker start "$VICTIM" >/dev/null
wait "$LT" || true
cat "$OUT"
python3 - "$OUT" "$TASKS" <<'PY'
import json, sys
out = json.load(open(sys.argv[1])); total = int(sys.argv[2])
print("completed %d/%d · e2e p50 %.1fs p99 %.1fs" % (out["completed"], total,
      out["e2e_latency_ms"]["p50"] / 1000, out["e2e_latency_ms"]["p99"] / 1000))
sys.exit(0 if out["completed"] == total else 1)
PY
echo "✅ CHAOS-LOAD PASS:杀掉一个 worker,$TASKS 个任务零丢失"
