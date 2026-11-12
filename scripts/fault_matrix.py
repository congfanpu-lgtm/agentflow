#!/usr/bin/env python3
"""故障矩阵:匀速提交 ECHO_BATCH 任务,中途 kill -9 一个组件,过一会儿拉起,回答三个问题:
丢没丢(收下的任务是否都 COMPLETED)、重没重(同一子任务是否被执行多次)、多久恢复。

在隔离的 compose 项目里跑(docker/chaos.yml:独立容器名和数据卷),不碰日常那套容器:
  C() { docker compose -p agentflow-chaos -f docker-compose.yml -f docker/chaos.yml "$@"; }
  C up -d --wait mysql redis mongo kafka && C up -d --build --no-deps server worker-1 worker-2
  python3 scripts/fault_matrix.py --victim server
  python3 scripts/fault_matrix.py --victim all          # worker-1 server kafka mysql redis mongo 依次
  python3 scripts/fault_matrix.py --self-check

"收下" = 提交接口返回了 taskUuid。故障期间被拒绝的提交(连接失败 / 5xx)不算丢,单独计数。
重复执行从 trace 里数(同一子任务多于 1 条 PROCESSED/OK),所以杀 mongo 那一行的重复数只是下限。
"""
import argparse
import json
import os
import subprocess
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from load_test import _get, _post, percentile  # noqa: E402

ALL = ["worker-1", "server", "kafka", "mysql", "redis", "mongo"]
TERMINAL = ("COMPLETED", "FAILED", "PARTIAL_FAILED")


def run_task(base, items, t0, stop_at):
    """提交 1 个任务并轮询到终态;任何网络错误都不让线程死掉。时间都是相对 t0 的秒数。"""
    rec = {"uuid": None, "t_submit": time.time() - t0, "status": "REJECTED", "t_done": None, "error": None}
    body = {"type": "ECHO_BATCH", "payload": {"items": [f"item-{i}" for i in range(items)]}}
    try:
        rec["uuid"] = _post(f"{base}/tasks", body, timeout=max(1, stop_at - time.time()))["taskUuid"]
    except Exception as e:  # 没拿到 taskUuid = 没被收下
        rec["error"] = type(e).__name__ + ": " + str(e)[:80]
        return rec
    rec["status"] = "UNKNOWN"
    while time.time() < stop_at:
        try:
            rec["status"] = _get(f"{base}/tasks/{rec['uuid']}", timeout=5)["status"]
        except Exception:
            pass  # server / MySQL 挂着的时候查不到,等它回来
        if rec["status"] in TERMINAL:
            rec["t_done"] = time.time() - t0
            break
        time.sleep(0.5)
    return rec


def trace_counts(events):
    """一个任务的 trace → (重复执行次数, 幂等拦截次数, trace 是否完整)。"""
    ok = {}
    skipped = 0
    for e in events:
        if e["stage"] == "PROCESSED" and e.get("status") == "OK":
            ok[e["subtaskId"]] = ok.get(e["subtaskId"], 0) + 1
        elif e["stage"] == "PROCESSED" and e.get("status") == "SKIPPED_IDEMPOTENT":
            skipped += 1
    stages = {e["stage"] for e in events}
    return sum(n - 1 for n in ok.values()), skipped, {"SUBMITTED", "TASK_FINALIZED"} <= stages


def summarize(recs, t_kill, t_start):
    accepted = [r for r in recs if r["uuid"]]
    done = [r for r in accepted if r["status"] == "COMPLETED"]
    before = [r for r in accepted if r["t_submit"] < t_start]      # 故障前和故障期间收下的
    after = [r["t_done"] for r in accepted if r["t_done"] and r["t_done"] > t_start]
    e2e = [r["t_done"] - r["t_submit"] for r in done]
    return {
        "submitted": len(recs), "accepted": len(accepted), "rejected": len(recs) - len(accepted),
        "completed": len(done),
        "failed": sum(1 for r in accepted if r["status"] in ("FAILED", "PARTIAL_FAILED")),
        "stuck": sum(1 for r in accepted if r["status"] not in TERMINAL),   # 到截止时间还没终态
        # 拉起后多久出第一个结果 / 多久把故障前收下的任务全部做完(有没做完的就是 None)
        "resume_s": round(min(after) - t_start, 1) if after else None,
        "drain_s": round(max(r["t_done"] for r in before) - t_start, 1)
        if before and all(r["t_done"] for r in before) else None,
        "e2e_p50_s": round(percentile(e2e, 0.5), 1), "e2e_p99_s": round(percentile(e2e, 0.99), 1),
        "t_kill": round(t_kill, 1), "t_start": round(t_start, 1),
    }


def drill(args, victim):
    container = f"{args.project}-{victim}-1"   # 直接操作容器:compose start 会去检查用不到的依赖(rag)
    t0 = time.time()
    stop_at = t0 + args.seconds + args.deadline
    mark = {}

    def fault():
        time.sleep(args.kill_after)
        subprocess.run(["docker", "kill", container], check=True, capture_output=True)
        mark["kill"] = time.time() - t0
        time.sleep(args.down)
        subprocess.run(["docker", "start", container], check=True, capture_output=True)
        mark["start"] = time.time() - t0

    threading.Thread(target=fault, daemon=True).start()
    n = int(args.rate * args.seconds)
    with ThreadPoolExecutor(max_workers=n) as ex:
        futs = []
        for i in range(n):   # 匀速提交:每个任务一个线程,卡住的提交不拖慢后面的
            time.sleep(max(0, t0 + i / args.rate - time.time()))
            futs.append(ex.submit(run_task, args.base, args.items, t0, stop_at))
        recs = [f.result() for f in futs]

    out = summarize(recs, mark["kill"], mark["start"])
    dups = skipped = complete = 0
    for r in recs:
        if r["uuid"]:
            try:
                d, s, c = trace_counts(_get(f"{args.base}/tasks/{r['uuid']}/trace"))
                dups, skipped, complete = dups + d, skipped + s, complete + c
            except Exception:
                pass
    out.update(victim=victim, duplicate_runs=dups, idempotent_skips=skipped,
               trace_complete=round(complete / out["accepted"], 3) if out["accepted"] else 0.0,
               rejected_errors=sorted({r["error"] for r in recs if r["error"]})[:3],
               not_completed=[r["uuid"] for r in recs if r["uuid"] and r["status"] != "COMPLETED"])
    return out


def self_check():
    recs = [{"uuid": "a", "t_submit": 1, "status": "COMPLETED", "t_done": 3, "error": None},
            {"uuid": "b", "t_submit": 12, "status": "COMPLETED", "t_done": 50, "error": None},
            {"uuid": None, "t_submit": 20, "status": "REJECTED", "t_done": None, "error": "URLError"},
            {"uuid": "c", "t_submit": 45, "status": "RUNNING", "t_done": None, "error": None}]
    s = summarize(recs, 10, 40)
    assert (s["accepted"], s["rejected"], s["completed"], s["stuck"]) == (3, 1, 2, 1), s
    assert s["resume_s"] == 10 and s["drain_s"] == 10 and s["e2e_p99_s"] == 38, s
    assert summarize(recs[3:], 10, 50)["drain_s"] is None     # 故障前收下的没做完 → 没恢复
    ev = [{"stage": "SUBMITTED"}, {"stage": "PROCESSED", "status": "OK", "subtaskId": 1},
          {"stage": "PROCESSED", "status": "OK", "subtaskId": 1},
          {"stage": "PROCESSED", "status": "SKIPPED_IDEMPOTENT", "subtaskId": 2}, {"stage": "TASK_FINALIZED"}]
    assert trace_counts(ev) == (1, 1, True) and trace_counts(ev[:2]) == (0, 0, False)
    print("self-check OK")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--victim", default="all", help="compose 服务名,或 all")
    ap.add_argument("--base", default="http://127.0.0.1:18080/api/v1")
    ap.add_argument("--project", default="agentflow-chaos")
    ap.add_argument("--rate", type=float, default=2.0, help="每秒提交几个任务")
    ap.add_argument("--seconds", type=float, default=60.0, help="提交持续多久")
    ap.add_argument("--items", type=int, default=5)
    ap.add_argument("--kill-after", type=float, default=10.0)
    ap.add_argument("--down", type=float, default=30.0, help="挂多久再拉起")
    ap.add_argument("--deadline", type=float, default=240.0, help="提交结束后最多再等多久")
    ap.add_argument("--out", help="把结果追加写成 JSON 行")
    ap.add_argument("--self-check", action="store_true")
    args = ap.parse_args()
    if args.self_check:
        return self_check()
    for victim in (ALL if args.victim == "all" else [args.victim]):
        out = drill(args, victim)
        print(json.dumps(out, ensure_ascii=False), flush=True)
        if args.out:
            with open(args.out, "a") as f:
                f.write(json.dumps(out, ensure_ascii=False) + "\n")
        time.sleep(20)   # 让上一轮的重平衡 / 重连收尾


if __name__ == "__main__":
    main()
