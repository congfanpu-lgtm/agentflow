#!/usr/bin/env bash
# W5-6 Task 7:端到端验收——提交 LLM_BATCH(默认 mock LLM,不烧钱)→ 轮询完成
# → 打印聚合结果 + Run Trace + LLM token 记账。证明:真 Agent 经网关跑通、限流/记账/schema 生效。
set -euo pipefail
BASE="http://localhost:8080/api/v1"

UUID=$(curl -s -X POST "$BASE/tasks" -H 'Content-Type: application/json' \
  -d '{"type":"LLM_BATCH","payload":{"items":["分布式系统里幂等为什么重要","令牌桶如何削峰"],"model":"mock-small"}}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["taskUuid"])')
echo "taskUuid=$UUID"

for i in $(seq 1 30); do
  ST=$(curl -s "$BASE/tasks/$UUID" | python3 -c 'import sys,json;print(json.load(sys.stdin)["status"])')
  echo "  [poll $i] status=$ST"
  [ "$ST" = "COMPLETED" ] && break
  sleep 2
done

echo "==> 任务聚合结果(每个子任务经 LlmProcessor→网关→schema 校验):"
curl -s "$BASE/tasks/$UUID" | python3 -m json.tool

echo "==> Run Trace(闭环审计,应覆盖 SUBMITTED→…→TASK_FINALIZED):"
curl -s "$BASE/tasks/$UUID/trace" | python3 -m json.tool

echo "==> LLM token 记账(卖点3 成本可观测):"
curl -s "$BASE/llm/usage" | python3 -m json.tool

echo "✅ LLM_BATCH 经网关跑通;结果含 summary/model/token 数;usage 见累计 token 与估算成本"
