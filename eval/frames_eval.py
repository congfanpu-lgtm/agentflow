#!/usr/bin/env python3
"""RESEARCH_AGENT 在多跳问题上的评测(零依赖,复用 run_eval.py 的提交/解析/模型客户端)。

题目来自 FRAMES(Google,824 道,每题要用到 2~15 篇 Wikipedia 文章,带标准答案):
  curl -L -o /tmp/frames.tsv https://huggingface.co/datasets/google/frames-benchmark/resolve/main/test.tsv
数据集不进仓库;按固定种子打乱后取一段,所以同样的 --offset/--n 每次是同一批题。

和 run_eval.py 的区别:那边的题一两页资料就能答,量的是报告的覆盖和引用;这里量的是"答对没有":
  agent        评审模型对照标准答案判 correct / incorrect(说"无法确定"算错)
  closed-book  同一个模型不给任何工具直接答(--closed-book),区分"查出来的"和"本来就记得的"
  gold seen    标准答案用到的 Wikipedia 条目里,agent 接触过几篇(trace 里工具结果预览只有 300 字,所以偏低)

用法(仓库根目录,服务已启动):
  python3 eval/frames_eval.py --tsv /tmp/frames.tsv --label dev --closed-book
  python3 eval/frames_eval.py --tsv /tmp/frames.tsv --label dev-rounds0 --payload '{"rounds":0}'
  python3 eval/frames_eval.py --tsv /tmp/frames.tsv --label holdout --offset 30 --closed-book
结果写到 eval/results/frames-<label>.json。
"""
import argparse
import ast
import csv
import json
import os
import random
import re
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, HERE)
import run_eval as E  # noqa: E402

SEED = 20261110
MAX_QUESTION_CHARS = 500   # 服务端对 payload.question 的上限
URL = re.compile(r"https?://[^\s\"'<>\\)\]]+")
GRADE = ("You grade answers to factual questions. Given a question, the ground-truth answer and a response, "
         "decide whether the response's final answer matches the ground truth in meaning (same entity, number or date; "
         "wording differences are fine). A response that gives a different value, hedges between several answers, "
         "or says it cannot determine the answer is incorrect. "
         'Reply with JSON only: {"verdict":"correct|incorrect","reason":"<15 words"}')
CLOSED_BOOK = ("Answer the question from your own knowledge. Think step by step, then commit to one final answer. "
               'Reply with JSON only: {"reasoning":"<80 words","answer":"..."}')


def wiki_key(url):
    """en.wikipedia.org/wiki/Harriet_Lane#x → 'harriet lane';不是维基条目返回 None。"""
    m = re.search(r"wikipedia\.org/wiki/([^#?]+)", url or "")
    return urllib.parse.unquote(m.group(1)).replace("_", " ").strip().lower() if m else None


def gold_links(row):
    try:
        links = ast.literal_eval(row.get("wiki_links") or "[]")
    except (ValueError, SyntaxError):
        links = []
    links = links or [v for k, v in row.items() if k.startswith("wikipedia_link") and v]
    return sorted({k for k in map(wiki_key, links) if k})


def seen_articles(trace, sources):
    """agent 接触过的维基条目:工具参数和结果预览里的 URL、最终来源。同时数工具调用次数。"""
    seen, calls = set(), 0
    for e in trace:
        if e.get("stage") != "AGENT_STEP":
            continue
        for c in json.loads(e.get("detail") or "{}").get("calls", []):
            calls += 1
            for u in URL.findall((c.get("args") or "") + " " + (c.get("resultPreview") or "")):
                seen.add(wiki_key(u))
    seen.update(wiki_key(s.get("url")) for s in sources or [])
    seen.discard(None)
    return seen, calls


def grade(grader, question, gold, response):
    if not (response or "").strip():
        return "incorrect", "empty response"
    try:
        r = grader.ask(GRADE, "Question: %s\n\nGround truth: %s\n\nResponse:\n%s" % (question, gold, response[:8000]), 200)
        return ("correct" if r.get("verdict") == "correct" else "incorrect"), r.get("reason", "")
    except Exception as e:   # 评审挂了单独记,不混进"答错"
        return "error", type(e).__name__


def run_task(base, question, payload, timeout):
    t0 = time.time()
    body = {"type": "RESEARCH_AGENT", "payload": {"question": question, **payload}}
    for attempt in range(3):   # 服务刚重启时提交会被断开;结果要等全部跑完才落盘,不能因为一次抖动整轮作废
        try:
            uuid = E.http_json(base + "/api/v1/tasks", body)["taskUuid"]
            break
        except Exception:
            if attempt == 2:
                raise
            time.sleep(5)
    task = {}
    while time.time() - t0 < timeout:
        time.sleep(3)
        try:
            task = E.http_json(base + "/api/v1/tasks/" + uuid)
        except Exception:
            continue
        if task.get("status") in E.TERMINAL:
            break
    x = E.extract(task.get("result"))
    reflects = [st.get("output") or {} for st in task.get("result") or [] if st.get("kind") == "REFLECT"]
    try:
        trace = E.http_json(base + "/api/v1/tasks/%s/trace" % uuid)
    except Exception:
        trace = []
    return {"taskUuid": uuid, "status": task.get("status") if task.get("status") in E.TERMINAL else "TIMEOUT",
            "latencySec": round(time.time() - t0, 1), "subQuestions": x["subQuestions"], "agents": x["agents"],
            "usage": x["usage"], "report": x["report"], "sources": x["sources"],
            "rounds": len(reflects), "followUps": [r.get("gaps") for r in reflects if isinstance(r, dict)]}, trace


def summarize(recs):
    n = len(recs)
    steps = [a.get("steps") or 0 for r in recs for a in r["agents"]]
    lat = sorted(r["latencySec"] for r in recs)
    s = {"n": n, "agentCorrect": sum(r["agentVerdict"] == "correct" for r in recs),
         "completed": sum(r["status"] == "COMPLETED" for r in recs),
         "tokensPerQuestion": sum(r["usage"]["promptTokens"] + r["usage"]["completionTokens"] for r in recs) // max(1, n),
         "latencyP50": lat[n // 2] if lat else None, "roundsPerQuestion": round(sum(r["rounds"] for r in recs) / max(1, n), 1),
         "agents": len(steps), "avgSteps": round(sum(steps) / max(1, len(steps)), 1),
         "agentsNoSource": sum(not a.get("sources") for r in recs for a in r["agents"]),
         "goldSeen": sum(len(r["goldSeen"]) for r in recs), "goldTotal": sum(len(r["goldLinks"]) for r in recs)}
    if any("closedVerdict" in r for r in recs):
        s["closedBookCorrect"] = sum(r.get("closedVerdict") == "correct" for r in recs)
    return s


def self_check():
    assert wiki_key("https://en.wikipedia.org/wiki/Harriet_Lane#Early_life") == "harriet lane"
    assert wiki_key("https://example.com/x") is None
    row = {"wiki_links": "['https://en.wikipedia.org/wiki/A_B', 'https://en.wikipedia.org/wiki/C']"}
    assert gold_links(row) == ["a b", "c"]
    assert gold_links({"wiki_links": "", "wikipedia_link_1": "https://en.wikipedia.org/wiki/Z"}) == ["z"]
    trace = [{"stage": "AGENT_STEP", "detail": json.dumps({"calls": [
        {"tool": "web_fetch", "args": '{"url": "https://en.wikipedia.org/wiki/A_B"}', "resultPreview": "x"},
        {"tool": "wiki_search", "args": "{}", "resultPreview": "URL: https://en.wikipedia.org/wiki/C\nSummary"}]})}]
    assert seen_articles(trace, [{"url": "https://en.wikipedia.org/wiki/Z"}]) == ({"a b", "c", "z"}, 2)
    recs = [{"agentVerdict": "correct", "closedVerdict": "incorrect", "status": "COMPLETED", "latencySec": 9.0, "rounds": 2,
             "usage": {"promptTokens": 90, "completionTokens": 10}, "agents": [{"steps": 3, "sources": 1}, {"steps": 1, "sources": 0}],
             "goldSeen": ["a"], "goldLinks": ["a", "b"]}]
    s = summarize(recs)
    assert (s["agentCorrect"], s["closedBookCorrect"], s["tokensPerQuestion"], s["agentsNoSource"], s["goldSeen"], s["goldTotal"]) \
        == (1, 0, 100, 1, 1, 2), s
    print("self-check ok")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--tsv", help="FRAMES 的 test.tsv 路径")
    ap.add_argument("--base", default="http://localhost:8080")
    ap.add_argument("--label", default="run")
    ap.add_argument("--n", type=int, default=30)
    ap.add_argument("--offset", type=int, default=0, help="从打乱后的第几题开始取(0 = 调参用的那批,30 = 留出的另一批)")
    ap.add_argument("--payload", default="{}", help='附加到任务 payload 的 JSON,例如 \'{"rounds":0}\'')
    ap.add_argument("--closed-book", action="store_true", help="同时让被测模型不带工具直接答")
    ap.add_argument("--concurrency", type=int, default=3)
    ap.add_argument("--timeout", type=int, default=600)
    ap.add_argument("--judge-model", default=os.environ.get("JUDGE_MODEL", "claude-sonnet-5-5"))
    ap.add_argument("--judge-extra-body", default=os.environ.get("JUDGE_EXTRA_BODY"))
    ap.add_argument("--self-check", action="store_true")
    a = ap.parse_args()
    if a.self_check:
        return self_check()
    if not a.tsv:
        ap.error("--tsv 必填(下载方式见文件开头)")
    os.environ.update(E.read_env_file(os.path.join(HERE, "..", ".env")))
    rows = [r for r in csv.DictReader(open(a.tsv, encoding="utf-8"), delimiter="\t") if len(r["Prompt"]) <= MAX_QUESTION_CHARS]
    random.Random(SEED).shuffle(rows)
    rows = rows[a.offset:a.offset + a.n]
    payload = json.loads(a.payload)
    grader = E.Judge(a.judge_model, a.judge_extra_body)
    model = E.Judge(os.environ["LLM_MODEL"]) if a.closed_book else None
    out = os.path.join(HERE, "results", "frames-%s.json" % a.label)
    print("running %d questions (offset %d), payload=%s" % (len(rows), a.offset, json.dumps(payload)), flush=True)

    def job(row):
        q, gold = row["Prompt"], row["Answer"]
        rec, trace = run_task(a.base.rstrip("/"), q, payload, a.timeout)
        g = gold_links(row)
        seen, calls = seen_articles(trace, rec["sources"])
        rec.update(id="frames-" + row[""], category=row["reasoning_types"], question=q, gold=gold,
                   goldLinks=g, goldSeen=sorted(set(g) & seen), toolCalls=calls)
        rec["agentVerdict"], rec["agentReason"] = grade(grader, q, gold, rec["report"])
        if model:
            try:
                rec["closedBook"] = str(model.ask(CLOSED_BOOK, q, 600).get("answer", ""))
            except Exception as e:
                rec["closedBook"], rec["closedBookError"] = "", type(e).__name__
            rec["closedVerdict"], rec["closedReason"] = grade(grader, q, gold, rec["closedBook"])
        print("  %-11s %-10s %5ss agent=%-9s closed=%-9s rounds=%d steps=%s gold=%d/%d" % (
            rec["id"], rec["status"], rec["latencySec"], rec["agentVerdict"], rec.get("closedVerdict", "-"),
            rec["rounds"], [x.get("steps") for x in rec["agents"]], len(rec["goldSeen"]), len(g)), flush=True)
        return rec

    with ThreadPoolExecutor(a.concurrency) as ex:
        recs = list(ex.map(job, rows))
    meta = {"label": a.label, "date": time.strftime("%Y-%m-%d %H:%M"), "model": os.environ.get("LLM_MODEL", "?"),
            "judge": grader.model, "seed": SEED, "offset": a.offset, "n": len(recs), "payload": payload}
    s = summarize(recs)
    with open(out, "w", encoding="utf-8") as f:
        json.dump({"meta": meta, "summary": s, "records": recs}, f, ensure_ascii=False, indent=1)
    print(json.dumps(s, ensure_ascii=False))


if __name__ == "__main__":
    sys.exit(main())
