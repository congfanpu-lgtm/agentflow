#!/usr/bin/env python3
"""RESEARCH_AGENT 评测(零依赖,标准库)。

对 eval/questions.json 里的每个问题提交一次真实研究任务,等它跑完,然后算:

  确定性指标(不调模型)
    completion   任务是否在时限内 COMPLETED
    latency      提交 → 终态的墙钟时间(p50 / p95)
    cost/tokens  各阶段 usage 求和(costMicro 按 LLM_PRICE_PER1K 估算)
    citations    引用密度(带 [n] 的句子占比)、来源利用率(被引用的来源 / 全部来源)
    lang_match   报告语言是否与问题语言一致
  LLM 评审指标(--judge,默认开)
    coverage     问题预设的 mustCover 要点逐条打分:0 没提到 / 1 一笔带过 / 2 讲透且有具体细节
                 coverage = 得分 ≥1 的占比;depth = 总分 / 满分(二元覆盖率在基线就到 100%,区分不出好坏)
    citation     抽样带引用的句子,抓取被引来源的网页正文,评审模型判定"来源是否支持这句话"
                 accuracy = supported / 可核验数;抓不到正文(付费墙、403)的记为 unverifiable,不计分

用法(在仓库根目录,先 docker compose up -d):
  评审模型复用 .env 里的 OPENAI_BASE_URL / OPENAI_API_KEY / OPENAI_EXTRA_BODY,脚本按字面读取 .env
  (和 docker compose 一致;不要 source,shell 会吃掉 JSON 里的引号)
  python3 eval/run_eval.py --label baseline [--limit 3] [--concurrency 3] [--no-judge]
  python3 eval/run_eval.py --label reflect --reflect   # 打开反思阶段(默认关闭)做对照
  python3 eval/run_eval.py --render eval/results/x.json   # 指标代码改了:用已有结果重算汇总
  python3 eval/run_eval.py --self-check     # 离线自检纯函数
结果写到 eval/results/<label>.json 与 <label>.md。
"""
import argparse
import html
import json
import math
import os
import re
import sys
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

HERE = os.path.dirname(os.path.abspath(__file__))
TERMINAL = {"COMPLETED", "FAILED", "PARTIAL_FAILED", "CANCELLED"}
CITE = re.compile(r"\[(\d{1,3})\]")
CJK = re.compile(r"[一-鿿]")
UA = "AgentFlow-eval/1.0 (research agent evaluation)"
SOURCES_HEAD = re.compile(r"^(#{1,6}\s*|\*\*)?(sources|references|参考来源|参考资料|参考文献|来源|引用)(\*\*)?\s*[:：]?\s*$", re.I)
SOURCE_LINE = re.compile(r"^[-*]?\s*\[\d{1,3}\]\s*(\S.*)?https?://\S+\s*$")


# ---------------------------------------------------------------- HTTP / 配置

def read_env_file(path):
    """按字面解析 KEY=VALUE(docker compose 的 .env 语义),不做 shell 展开。"""
    env = {}
    if os.path.exists(path):
        for line in open(path, encoding="utf-8"):
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                env[k.strip()] = v.strip()
    return env


def http_json(url, body=None, headers=None, timeout=30):
    data = None if body is None else json.dumps(body).encode()
    h = {"Content-Type": "application/json", **(headers or {})}
    req = urllib.request.Request(url, data=data, headers=h)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.load(r)


# ---------------------------------------------------------------- 纯函数(--self-check 覆盖)

def percentile(values, q):
    """nearest-rank:排序后取第 ceil(q*n) 个(1-indexed)。"""
    if not values:
        return 0.0
    s = sorted(values)
    return s[max(0, min(len(s) - 1, math.ceil(q * len(s)) - 1))]


def is_cjk(text, threshold=0.15):
    letters = [c for c in text if c.isalpha()]
    return bool(letters) and len(CJK.findall(text)) / len(letters) >= threshold


def sentences(report):
    """正文句子:去掉标题/来源区,按中英文句末标点和换行切分,过短的碎片不算。"""
    out = []
    for line in report.splitlines():
        line = line.strip()
        if SOURCES_HEAD.match(line):
            break   # 模型自己写的来源清单:不是正文
        if not line or line.startswith("#") or SOURCE_LINE.match(line):
            continue
        line = re.sub(r"^([-*]|\d+[.)])\s+", "", line)
        for s in re.split(r"(?<=[。！？!?])|(?<=\.)\s+(?=[A-Z一-鿿*])", line):
            s = s.strip()
            if len(s) >= 12:
                out.append(s)
    return out


def cited_sentences(report):
    """[(句子, [n, ...])],只取带引用的句子。"""
    res = []
    for s in sentences(report):
        ns = sorted({int(n) for n in CITE.findall(s)})
        if ns:
            res.append((s, ns))
    return res


def evenly_sample(items, k):
    """确定性抽样:等间隔取 k 个,两次评测抽到同样位置的句子,便于前后对比。"""
    if len(items) <= k:
        return list(items)
    step = len(items) / k
    return [items[int(i * step)] for i in range(k)]


def html_to_text(raw):
    s = re.sub(r"(?is)<(script|style|noscript|svg|nav|header|footer|aside)\b.*?</\1>", " ", raw)
    m = re.search(r"(?is)<(main|article)\b[^>]*>(.*)</\1>", s)
    if m:
        s = m.group(2)
    s = re.sub(r"(?s)<[^>]+>", " ", s)
    return re.sub(r"\s+", " ", html.unescape(s)).strip()


def parse_json_obj(text):
    """评审模型偶尔包 ```json 围栏或前后带话,取第一个 {...}。"""
    m = re.search(r"\{.*\}", text or "", re.S)
    if not m:
        raise ValueError("no JSON object in: %r" % (text or "")[:200])
    return json.loads(m.group(0))


def extract(result):
    """task.result(按 seq 的子任务数组)→ plan / agents / report / sources / usage 合计。"""
    plan, agents, synth, reflect = None, [], None, None
    usage = {"calls": 0, "promptTokens": 0, "completionTokens": 0, "costMicro": 0}
    for st in result or []:
        out = st.get("output") if isinstance(st.get("output"), dict) else {}
        for k in usage:
            usage[k] += int((out.get("usage") or {}).get(k, 0) or 0)
        kind = st.get("kind")
        if kind == "PLAN":
            plan = out
        elif kind == "AGENT":
            agents.append({"status": st.get("status"), "steps": out.get("steps"),
                           "sources": len(out.get("sources") or []), "error": st.get("error")})
        elif kind == "SYNTHESIZE":
            synth = out
        elif kind == "REFLECT":
            reflect = out
    return {"subQuestions": (plan or {}).get("subQuestions", []), "agents": agents,
            "report": (synth or {}).get("report", ""), "sources": (synth or {}).get("sources", []),
            "usage": usage, "reflectGaps": None if reflect is None else len(reflect.get("gaps") or [])}


def citation_stats(report, sources):
    sents = sentences(report)
    cited = cited_sentences(report)
    used = {n for _, ns in cited for n in ns}
    valid = {s.get("n") for s in sources}
    return {"sentences": len(sents), "citedSentences": len(cited),
            "density": round(len(cited) / len(sents), 3) if sents else 0.0,
            "sources": len(sources),
            "utilization": round(len(used & valid) / len(valid), 3) if valid else 0.0,
            "dangling": len(used - valid)}


def tool_stats(trace):
    calls, errors, dropped = {}, 0, 0
    for e in trace:
        if e.get("stage") == "CITATION_DROPPED":
            dropped += len(json.loads(e.get("detail") or "{}").get("urls", []))
        if e.get("stage") != "AGENT_STEP":
            continue
        d = json.loads(e.get("detail") or "{}")
        for c in d.get("calls", []):
            calls[c.get("tool")] = calls.get(c.get("tool"), 0) + 1
            if (c.get("resultPreview") or "").startswith("ERROR:"):
                errors += 1
    return {"calls": calls, "errors": errors, "citationsDropped": dropped}


# ---------------------------------------------------------------- 评审模型

class Judge:
    def __init__(self, model, extra_body=None):
        self.model = model
        self.base = os.environ.get("OPENAI_BASE_URL", "https://api.openai.com/v1").rstrip("/")
        self.key = os.environ.get("OPENAI_API_KEY", "")
        # 评审模型可能和被测模型要求不同的厂商参数(如 Sonnet 关思考用 between_tools),可单独指定
        extra = (extra_body if extra_body is not None else os.environ.get("OPENAI_EXTRA_BODY", "")).strip()
        self.extra = json.loads(extra) if extra else {}
        self.param = os.environ.get("OPENAI_MAX_TOKENS_PARAM", "max_tokens")
        if not self.key:
            raise SystemExit("评审需要 OPENAI_API_KEY(写在 .env 里),或加 --no-judge")

    def ask(self, system, user, max_tokens=400):
        body = {"model": self.model, self.param: max_tokens,
                "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}], **self.extra}
        for attempt in range(4):
            try:
                r = http_json(self.base + "/chat/completions", body,
                              {"Authorization": "Bearer " + self.key}, timeout=90)
                return parse_json_obj(r["choices"][0]["message"]["content"])
            except urllib.error.HTTPError as e:
                if e.code not in (429, 500, 502, 503, 529) or attempt == 3:
                    raise RuntimeError("judge HTTP %d" % e.code) from None   # 不回显响应体
                time.sleep(2 ** attempt * 3)
            except (ValueError, KeyError):
                if attempt == 3:
                    raise
        raise RuntimeError("unreachable")

    def coverage(self, question, aspects, report):
        """每个要点 0/1/2 分。"""
        listing = "\n".join("%d. %s" % (i + 1, a) for i, a in enumerate(aspects))
        r = self.ask("You grade research reports strictly. Score each required aspect: "
                     "0 = not addressed; 1 = mentioned or described only in general terms; "
                     "2 = explained with concrete specifics (numbers, dates, mechanisms, named examples) "
                     "that would satisfy an expert. "
                     'Reply with JSON only: {"scores":[0,1,2,...]} in the same order.',
                     "Question: %s\n\nRequired aspects:\n%s\n\nReport:\n%s" % (question, listing, report))
        sc = [max(0, min(2, int(x))) for x in r.get("scores", []) if isinstance(x, (int, float))][:len(aspects)]
        return sc + [0] * (len(aspects) - len(sc))

    def supports(self, sentence, source_title, source_text):
        r = self.ask("You check citations. Given a sentence from a report and the text of the source it cites, "
                     "decide whether the source supports the sentence's factual claims. "
                     '"supported": the key claims are stated or directly implied by the source. '
                     '"partial": some claims are supported but a number, date or detail is missing or different. '
                     '"unsupported": the source does not back the claim or contradicts it. '
                     'Reply with JSON only: {"verdict":"supported|partial|unsupported","reason":"<15 words"}',
                     "Sentence: %s\n\nSource title: %s\nSource text (truncated):\n%s"
                     % (sentence, source_title, source_text), max_tokens=200)
        v = r.get("verdict", "unsupported")
        return v if v in ("supported", "partial", "unsupported") else "unsupported", r.get("reason", "")


_page_cache = {}


def fetch_text(url, limit=12000):
    """抓被引来源的正文;失败或正文太短(付费墙、反爬、纯 JS 页)返回 None → unverifiable。"""
    if url in _page_cache:
        return _page_cache[url]
    text = None
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 " + UA})
        with urllib.request.urlopen(req, timeout=15) as r:
            if "html" in r.headers.get("Content-Type", "html") or "text" in r.headers.get("Content-Type", ""):
                raw = r.read(2_000_000).decode(r.headers.get_content_charset() or "utf-8", "replace")
                t = html_to_text(raw)
                text = t[:limit] if len(t) >= 400 else None
    except Exception:
        text = None
    _page_cache[url] = text
    return text


# ---------------------------------------------------------------- 单题流程

def run_one(base, q, timeout, judge, samples, reflect=False):
    t0 = time.time()
    rec = {"id": q["id"], "category": q["category"], "question": q["question"]}
    try:
        payload = {"question": q["question"], "reflect": reflect}
        uuid = http_json(base + "/api/v1/tasks", {"type": "RESEARCH_AGENT", "payload": payload})["taskUuid"]
    except Exception as e:
        rec.update(status="SUBMIT_ERROR", error=type(e).__name__)
        return rec
    rec["taskUuid"] = uuid
    task = {}
    while time.time() - t0 < timeout:
        time.sleep(3)
        try:
            task = http_json(base + "/api/v1/tasks/" + uuid)
        except Exception:
            continue
        if task.get("status") in TERMINAL:
            break
    rec["status"] = task.get("status") if task.get("status") in TERMINAL else "TIMEOUT"
    rec["latencySec"] = round(time.time() - t0, 1)
    x = extract(task.get("result"))
    try:
        trace = http_json(base + "/api/v1/tasks/%s/trace" % uuid)
    except Exception:
        trace = []
    rec.update(subQuestions=x["subQuestions"], agents=x["agents"], usage=x["usage"], reflectGaps=x["reflectGaps"],
               citations=citation_stats(x["report"], x["sources"]), tools=tool_stats(trace),
               langMatch=is_cjk(x["report"]) == is_cjk(q["question"]) if x["report"] else None,
               report=x["report"], sources=x["sources"])
    if judge and x["report"]:
        try:
            sc = judge.coverage(q["question"], q["mustCover"], x["report"])
            rec["coverage"] = {"scores": sc, "score": round(sum(s >= 1 for s in sc) / len(sc), 3),
                               "depth": round(sum(sc) / (2 * len(sc)), 3)}
        except Exception as e:
            rec["coverage"] = {"error": str(e)}
        by_n = {s["n"]: s for s in x["sources"]}
        checks = []
        for sent, ns in evenly_sample(cited_sentences(x["report"]), samples):
            src = by_n.get(ns[0])
            text = fetch_text(src["url"]) if src else None
            if text is None:
                checks.append({"sentence": sent, "n": ns[0], "verdict": "unverifiable"})
                continue
            try:
                v, why = judge.supports(sent, src.get("title", ""), text)
            except Exception as e:
                v, why = "error", str(e)
            checks.append({"sentence": sent, "n": ns[0], "url": src["url"], "verdict": v, "reason": why})
        rec["citationChecks"] = checks
    return rec


# ---------------------------------------------------------------- 汇总

def summarize(records):
    n = len(records)
    done = [r for r in records if r.get("status") == "COMPLETED"]
    lat = [r["latencySec"] for r in done]
    cost = [r["usage"]["costMicro"] / 1e6 for r in done]
    toks = [r["usage"]["promptTokens"] + r["usage"]["completionTokens"] for r in done]
    calls = [r["usage"]["calls"] for r in done]
    cov = [r["coverage"]["score"] for r in done if "score" in r.get("coverage", {})]
    verdicts = [c["verdict"] for r in done for c in r.get("citationChecks", [])]
    verifiable = [v for v in verdicts if v in ("supported", "partial", "unsupported")]
    lang = [r["langMatch"] for r in done if r.get("langMatch") is not None]
    mean = lambda xs: round(sum(xs) / len(xs), 3) if xs else None
    return {
        "questions": n, "completed": len(done),
        "completionRate": round(len(done) / n, 3) if n else 0,
        "latencyP50": percentile(lat, 0.5), "latencyP95": percentile(lat, 0.95),
        "costPerQuestionUsd": mean(cost), "tokensPerQuestion": mean(toks), "llmCallsPerQuestion": mean(calls),
        "citationDensity": mean([r["citations"]["density"] for r in done]),
        "sourceUtilization": mean([r["citations"]["utilization"] for r in done]),
        "sourcesPerReport": mean([r["citations"]["sources"] for r in done]),
        "danglingCitations": sum(r["citations"]["dangling"] for r in done),
        "langMatchRate": mean([1.0 if x else 0.0 for x in lang]),
        "coverage": mean(cov),
        "depth": mean([r["coverage"]["depth"] for r in done if "depth" in r.get("coverage", {})]),
        "agentsPerQuestion": mean([len(r.get("agents", [])) for r in done]),
        # agent 最终答案解析失败(如被截断)时来源全丢,这个比例能直接暴露它
        "agentsWithoutSources": mean([1.0 if not a.get("sources") else 0.0
                                      for r in done for a in r.get("agents", []) if a.get("status") == "COMPLETED"]),
        "reflectGapsPerQuestion": mean([r["reflectGaps"] for r in done if r.get("reflectGaps") is not None]),
        "citationChecks": len(verdicts), "citationVerifiable": len(verifiable),
        "citationAccuracyStrict": round(verifiable.count("supported") / len(verifiable), 3) if verifiable else None,
        "citationAccuracyLenient": round((verifiable.count("supported") + verifiable.count("partial")) / len(verifiable), 3) if verifiable else None,
        "toolErrors": sum(r["tools"]["errors"] for r in done),
        "citationsDroppedByGuard": sum(r["tools"]["citationsDropped"] for r in done),
    }


def to_markdown(label, meta, s, records):
    pct = lambda v: "—" if v is None else "%.0f%%" % (v * 100)
    lines = ["# Eval: %s" % label, "",
             "%s · model `%s` · judge `%s` · %d questions" % (meta["date"], meta["model"], meta["judge"], s["questions"]), "",
             "| Metric | Value |", "|---|---|",
             "| Completion rate | %s (%d/%d) |" % (pct(s["completionRate"]), s["completed"], s["questions"]),
             "| Latency p50 / p95 | %.0fs / %.0fs |" % (s["latencyP50"], s["latencyP95"]),
             "| Cost per question (est.) | $%.4f |" % (s["costPerQuestionUsd"] or 0),
             "| Tokens / LLM calls per question | %s / %s |" % (s["tokensPerQuestion"], s["llmCallsPerQuestion"]),
             "| Aspect coverage / depth (judge, 0-1-2 rubric) | %s / %s |" % (pct(s["coverage"]), pct(s.get("depth"))),
             "| Agents per question (incl. follow-ups) / review gaps | %s / %s |" % (s.get("agentsPerQuestion"), s.get("reflectGapsPerQuestion")),
             "| Citation accuracy strict / lenient (judge) | %s / %s (%d verifiable of %d sampled) |" % (
                 pct(s["citationAccuracyStrict"]), pct(s["citationAccuracyLenient"]), s["citationVerifiable"], s["citationChecks"]),
             "| Citation density (sentences with [n]) | %s |" % pct(s["citationDensity"]),
             "| Source utilization | %s |" % pct(s["sourceUtilization"]),
             "| Sources per report / agents with no sources | %s / %s |" % (s["sourcesPerReport"], pct(s.get("agentsWithoutSources"))),
             "| Report language matches question | %s |" % pct(s["langMatchRate"]),
             "| Tool errors / citations dropped by guard | %d / %d |" % (s["toolErrors"], s["citationsDroppedByGuard"]),
             "", "| id | status | latency | depth | citations ok/checked | sources | gaps | lang |", "|---|---|---|---|---|---|---|---|"]
    for r in records:
        checks = r.get("citationChecks", [])
        ok = sum(c["verdict"] == "supported" for c in checks)
        ver = sum(c["verdict"] in ("supported", "partial", "unsupported") for c in checks)
        lines.append("| %s | %s | %ss | %s | %d/%d | %s | %s | %s |" % (
            r["id"], r.get("status"), r.get("latencySec", "—"),
            pct(r.get("coverage", {}).get("depth")), ok, ver,
            r.get("citations", {}).get("sources", "—"),
            "—" if r.get("reflectGaps") is None else r["reflectGaps"],
            {True: "ok", False: "MISMATCH", None: "—"}[r.get("langMatch")]))
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------- 入口

def self_check():
    assert percentile([1, 2, 3, 4], 0.5) == 2 and percentile([5], 0.95) == 5 and percentile([], 0.5) == 0.0
    assert percentile(list(range(1, 21)), 0.95) == 19
    assert is_cjk("今天纳斯达克下跌了 2%") and not is_cjk("Why did QQQ fall today?")
    assert not is_cjk("Kafka 4.0 removed ZooKeeper 完全")  # 少量中文夹在英文里不算中文报告
    rep = "## Title\n\nKafka 4.0 removed ZooKeeper entirely [1]. KRaft is now the only mode [1][2].\n" \
          "- Migration requires a bridge release first [3].\n纳指当日下跌 2.5%[2]。原因是芯片股走弱。\n" \
          "[2] https://cdp.cooley.com/digital-ai-omnibus\n### Sources\n[1] Kafka docs https://kafka.apache.org [9] trailing.\n"
    ss = sentences(rep)
    assert "Kafka 4.0 removed ZooKeeper entirely [1]." in ss, ss
    assert any(s.startswith("Migration requires") for s in ss), ss
    assert "原因是芯片股走弱。" not in ss  # 不足 12 字的碎片不算
    cs = cited_sentences(rep)
    assert [ns for _, ns in cs] == [[1], [1, 2], [3], [2]], cs
    st = citation_stats(rep, [{"n": 1}, {"n": 2}, {"n": 3}, {"n": 4}])
    assert st["citedSentences"] == 4 and st["utilization"] == 0.75 and st["dangling"] == 0, st
    assert citation_stats("x [9] long enough sentence here.", [{"n": 1}])["dangling"] == 1
    assert evenly_sample(list(range(10)), 4) == [0, 2, 5, 7] and evenly_sample([1, 2], 4) == [1, 2]
    assert html_to_text("<nav>menu</nav><main><p>Hello &amp; <b>world</b></p></main><footer>f</footer>") == "Hello & world"
    import tempfile
    with tempfile.NamedTemporaryFile("w", suffix=".env", delete=False) as f:
        f.write('# c\nOPENAI_EXTRA_BODY={"thinking":{"type":"disabled"}}\nEMPTY=\n')
    assert read_env_file(f.name) == {"OPENAI_EXTRA_BODY": '{"thinking":{"type":"disabled"}}', "EMPTY": ""}
    os.unlink(f.name)
    assert parse_json_obj('```json\n{"covered":[true,false]}\n```') == {"covered": [True, False]}
    x = extract([{"seq": 0, "kind": "PLAN", "output": {"subQuestions": ["a", "b"], "usage": {"calls": 1, "costMicro": 10}}},
                 {"seq": 1, "kind": "AGENT", "status": "COMPLETED", "output": {"steps": 3, "sources": [{}], "usage": {"calls": 3, "costMicro": 20}}},
                 {"seq": 2, "kind": "SYNTHESIZE", "output": {"report": "r", "sources": [{"n": 1}], "usage": {"calls": 1, "costMicro": 5}}}])
    assert x["subQuestions"] == ["a", "b"] and x["report"] == "r" and x["usage"]["calls"] == 5 and x["usage"]["costMicro"] == 35
    tr = [{"stage": "AGENT_STEP", "detail": json.dumps({"calls": [{"tool": "web_search", "resultPreview": "Title: x"},
                                                                    {"tool": "web_fetch", "resultPreview": "ERROR: web_fetch 调用失败"}]})},
          {"stage": "CITATION_DROPPED", "detail": json.dumps({"urls": ["u1", "u2"]})}]
    assert tool_stats(tr) == {"calls": {"web_search": 1, "web_fetch": 1}, "errors": 1, "citationsDropped": 2}
    recs = [{"status": "COMPLETED", "latencySec": 10, "usage": {"costMicro": 50000, "promptTokens": 100, "completionTokens": 50, "calls": 4},
             "citations": {"density": 0.5, "utilization": 1.0, "sources": 3, "dangling": 0}, "langMatch": True,
             "coverage": {"score": 2 / 3, "depth": 0.5}, "tools": {"errors": 0, "citationsDropped": 1}, "agents": [{"status": "COMPLETED", "sources": 2}, {"status": "COMPLETED", "sources": 0}], "reflectGaps": 1,
             "citationChecks": [{"verdict": "supported"}, {"verdict": "partial"}, {"verdict": "unverifiable"}, {"verdict": "unsupported"}]},
            {"status": "TIMEOUT"}]
    s = summarize(recs)
    assert s["completionRate"] == 0.5 and s["citationVerifiable"] == 3 and s["citationAccuracyStrict"] == 0.333
    assert s["citationAccuracyLenient"] == 0.667 and s["costPerQuestionUsd"] == 0.05 and s["coverage"] == 0.667, s
    assert s["depth"] == 0.5 and s["agentsPerQuestion"] == 2 and s["reflectGapsPerQuestion"] == 1, s
    assert s["agentsWithoutSources"] == 0.5, s
    assert extract([{"kind": "REFLECT", "output": {"gaps": ["x", "y"]}}])["reflectGaps"] == 2
    assert extract([])["reflectGaps"] is None
    print("self-check OK")


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--base", default="http://localhost:8080")
    ap.add_argument("--questions", default=os.path.join(HERE, "questions.json"))
    ap.add_argument("--label", default="run")
    ap.add_argument("--limit", type=int, default=0, help="只跑前 N 题(调试用)")
    ap.add_argument("--only", default="", help="逗号分隔的题目 id")
    ap.add_argument("--concurrency", type=int, default=3)
    ap.add_argument("--timeout", type=int, default=480, help="单题时限(秒)")
    ap.add_argument("--judge-model", default=os.environ.get("JUDGE_MODEL", "claude-sonnet-5-5"))
    ap.add_argument("--judge-extra-body", default=os.environ.get("JUDGE_EXTRA_BODY"),
                    help="评审模型的厂商参数 JSON,默认同 OPENAI_EXTRA_BODY")
    ap.add_argument("--samples", type=int, default=4, help="每篇报告抽查几句引用")
    ap.add_argument("--no-judge", action="store_true")
    ap.add_argument("--reflect", action="store_true", help="payload.reflect=true:打开反思阶段(服务端默认关闭)")
    ap.add_argument("--self-check", action="store_true")
    ap.add_argument("--render", metavar="RESULT_JSON", help="只用已有结果重新生成汇总和 .md(指标代码改了之后用)")
    ap.add_argument("--env-file", default=os.path.join(HERE, "..", ".env"))
    a = ap.parse_args()
    if a.self_check:
        return self_check()
    if a.render:
        d = json.load(open(a.render, encoding="utf-8"))
        d["summary"] = summarize(d["records"])
        with open(a.render, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=1)
        md = to_markdown(d["meta"]["label"], {"judge": "?", "model": "?", "date": "?", **d["meta"]}, d["summary"], d["records"])
        with open(a.render[:-len(".json")] + ".md", "w", encoding="utf-8") as f:
            f.write(md)
        return print(md.split("\n\n| id")[0])
    os.environ.update(read_env_file(a.env_file))

    qs = json.load(open(a.questions, encoding="utf-8"))
    if a.only:
        ids = set(a.only.split(","))
        qs = [q for q in qs if q["id"] in ids]
    if a.limit:
        qs = qs[:a.limit]
    judge = None if a.no_judge else Judge(a.judge_model, a.judge_extra_body)
    print("running %d questions, concurrency=%d, judge=%s" % (len(qs), a.concurrency, judge and judge.model), flush=True)

    def job(q):
        r = run_one(a.base.rstrip("/"), q, a.timeout, judge, a.samples, reflect=a.reflect)
        print("  %-18s %-10s %6ss depth=%s gaps=%s" % (r["id"], r.get("status"), r.get("latencySec"),
                                                        r.get("coverage", {}).get("depth"), r.get("reflectGaps")), flush=True)
        return r

    with ThreadPoolExecutor(a.concurrency) as ex:
        records = list(ex.map(job, qs))
    meta = {"label": a.label, "date": time.strftime("%Y-%m-%d %H:%M"), "model": os.environ.get("LLM_MODEL", "?"),
            "judge": judge.model if judge else "none", "concurrency": a.concurrency, "reflect": a.reflect}
    s = summarize(records)
    out = os.path.join(HERE, "results", a.label)
    os.makedirs(os.path.dirname(out), exist_ok=True)
    with open(out + ".json", "w", encoding="utf-8") as f:
        json.dump({"meta": meta, "summary": s, "records": records}, f, ensure_ascii=False, indent=1)
    md = to_markdown(a.label, meta, s, records)
    with open(out + ".md", "w", encoding="utf-8") as f:
        f.write(md)
    print(md)


if __name__ == "__main__":
    sys.exit(main())
