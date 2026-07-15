#!/usr/bin/env python3
"""W8 压测 harness(零依赖,标准库)。

测本项目"提交任务→异步完成"的**端到端**语义:并发提交 N 个任务(每任务 M 子任务),
并发轮询到 COMPLETED,汇总提交延迟 / 端到端完成延迟 P50/P99、吞吐(任务分、子任务秒)、
失败数。可选 --trace-check:逐任务查 /trace 算 trace 完整率(指标7)与幂等拦截数(指标3)。

压测用 mock LLM / ECHO_BATCH,测的是调度/并发/容错,不烧钱。数字与机器相关,报告需标注环境。

用法:
  python3 scripts/load_test.py --tasks 30 --items 5 --concurrency 10 [--type ECHO_BATCH] [--trace-check]
  python3 scripts/load_test.py --self-check      # 离线自检统计函数
"""
import argparse
import json
import math
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor


def _post(url, body, timeout=30):
    data = json.dumps(body).encode()
    req = urllib.request.Request(url, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)


def _get(url, timeout=30):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.load(r)


def percentile(values, q):
    """q in [0,1]。nearest-rank:排序后取 ceil(q*n) 位(1-indexed)。"""
    if not values:
        return 0.0
    s = sorted(values)
    if q <= 0:
        return s[0]
    rank = math.ceil(q * len(s))
    return s[min(rank, len(s)) - 1]


def _payload(task_type, m):
    items = [f"item-{i}" for i in range(m)]
    if task_type == "ECHO_BATCH":
        return {"type": "ECHO_BATCH", "payload": {"items": items}}
    if task_type == "LLM_BATCH":
        return {"type": "LLM_BATCH", "payload": {"items": items, "model": "mock-small"}}
    raise ValueError(f"unsupported type for load test: {task_type}")


def run_one(base, task_type, m, poll_interval, deadline_s):
    """提交 1 个任务并轮询到终态。返回 (submit_latency, e2e_latency, uuid, ok)。"""
    t0 = time.perf_counter()
    resp = _post(f"{base}/tasks", _payload(task_type, m))
    uuid = resp["taskUuid"]
    submit_latency = time.perf_counter() - t0
    ok = False
    while time.perf_counter() - t0 < deadline_s:
        st = _get(f"{base}/tasks/{uuid}")["status"]
        if st == "COMPLETED":
            ok = True
            break
        if st in ("FAILED", "PARTIAL_FAILED"):
            break
        time.sleep(poll_interval)
    e2e = time.perf_counter() - t0
    return submit_latency, e2e, uuid, ok


def trace_stats(base, uuids):
    """返回 (trace 完整率, 幂等拦截数, 总 PROCESSED 数)。"""
    complete = 0
    skipped = 0
    processed = 0
    for u in uuids:
        events = _get(f"{base}/tasks/{u}/trace")
        stages = {e["stage"] for e in events}
        if "SUBMITTED" in stages and "TASK_FINALIZED" in stages:
            complete += 1
        for e in events:
            if e["stage"] == "PROCESSED":
                processed += 1
                if e.get("status") == "SKIPPED_IDEMPOTENT":
                    skipped += 1
    rate = complete / len(uuids) if uuids else 0.0
    return rate, skipped, processed


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", default="http://localhost:8080/api/v1")
    ap.add_argument("--tasks", type=int, default=30)
    ap.add_argument("--items", type=int, default=5)
    ap.add_argument("--type", default="ECHO_BATCH")
    ap.add_argument("--concurrency", type=int, default=10)
    ap.add_argument("--poll-interval", type=float, default=0.2)
    ap.add_argument("--deadline", type=float, default=120.0)
    ap.add_argument("--trace-check", action="store_true")
    ap.add_argument("--self-check", action="store_true")
    args = ap.parse_args()

    if args.self_check:
        assert percentile([], 0.5) == 0.0
        assert percentile([10], 0.99) == 10
        assert percentile(list(range(1, 101)), 0.5) == 50   # nearest-rank
        assert percentile(list(range(1, 101)), 0.99) == 99
        assert percentile([5, 1, 3, 2, 4], 0.5) == 3
        print("self-check OK")
        return

    wall0 = time.perf_counter()
    results = []
    with ThreadPoolExecutor(max_workers=args.concurrency) as ex:
        futs = [ex.submit(run_one, args.base, args.type, args.items,
                          args.poll_interval, args.deadline)
                for _ in range(args.tasks)]
        for f in futs:
            results.append(f.result())
    wall = time.perf_counter() - wall0

    submits = [r[0] for r in results]
    e2es = [r[1] for r in results if r[3]]
    uuids = [r[2] for r in results]
    ok = sum(1 for r in results if r[3])
    failed = args.tasks - ok
    subtasks = args.tasks * args.items

    out = {
        "env": {"tasks": args.tasks, "items": args.items, "type": args.type,
                "concurrency": args.concurrency},
        "wall_s": round(wall, 2),
        "completed": ok,
        "failed": failed,
        "submit_latency_ms": {"p50": round(percentile(submits, 0.5) * 1000, 1),
                              "p99": round(percentile(submits, 0.99) * 1000, 1)},
        "e2e_latency_ms": {"p50": round(percentile(e2es, 0.5) * 1000, 1),
                           "p99": round(percentile(e2es, 0.99) * 1000, 1)},
        "throughput": {"tasks_per_min": round(ok / wall * 60, 1) if wall else 0,
                       "subtasks_per_sec": round(subtasks / wall, 1) if wall else 0},
    }
    if args.trace_check:
        rate, skipped, processed = trace_stats(args.base, uuids)
        out["trace_completeness"] = round(rate, 3)
        out["idempotency_skipped"] = skipped
        out["processed_total"] = processed
    print(json.dumps(out, indent=2, ensure_ascii=False))


if __name__ == "__main__":
    main()
