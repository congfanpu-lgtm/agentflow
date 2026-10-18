"""离线检索评测:在 BEIR 公开数据集上比较 BM25 / 语义向量 / 混合检索(RRF),用的是服务自己的排序代码。

dense 这里用 numpy 暴力算余弦,和 pgvector 不建 ANN 索引时 `<=>` 的精确检索是同一个计算。

    cd agentflow-rag
    .venv/bin/python retrieval_eval.py --datasets scifact,nfcorpus \\
        --models hash,BAAI/bge-small-en-v1.5 --out ../eval/results/retrieval-v1

数据集首次运行时下载到 ~/.cache/agentflow/beir(每个几 MB),不进仓库。
"""
import argparse
import json
import math
import time
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import numpy as np

from app import embedding
from app.rank import B, BM25, CANDIDATES, K1, RRF_K, rrf

BEIR_URL = "https://public.ukp.informatik.tu-darmstadt.de/thakur/BEIR/datasets/{}.zip"
CACHE = Path.home() / ".cache" / "agentflow" / "beir"
METRICS = ("ndcg@10", "recall@3", "recall@10")   # kb_search 默认给 agent top-3;10 是 BEIR 的标准截断


def ndcg(ranked: list[str], rel: dict[str, int], k: int = 10) -> float:
    """线性增益(与 BEIR / trec_eval 一致)。"""
    dcg = sum(rel.get(d, 0) / math.log2(i + 2) for i, d in enumerate(ranked[:k]))
    ideal = sorted(rel.values(), reverse=True)[:k]
    idcg = sum(g / math.log2(i + 2) for i, g in enumerate(ideal))
    return dcg / idcg if idcg else 0.0


def recall(ranked: list[str], rel: dict[str, int], k: int) -> float:
    return len(set(ranked[:k]) & rel.keys()) / len(rel)


def paired_ci(a: list[float], b: list[float], iters: int = 2000) -> tuple[float, float]:
    """按查询配对的 bootstrap:mean(a - b) 的 95% 置信区间。"""
    d = np.asarray(a, dtype=float) - np.asarray(b, dtype=float)
    means = np.random.default_rng(0).choice(d, size=(iters, len(d))).mean(axis=1)
    return float(np.percentile(means, 2.5)), float(np.percentile(means, 97.5))


def load(name: str):
    root = CACHE / name
    if not (root / "corpus.jsonl").exists():
        CACHE.mkdir(parents=True, exist_ok=True)
        z = CACHE / f"{name}.zip"
        urllib.request.urlretrieve(BEIR_URL.format(name), z)
        with zipfile.ZipFile(z) as f:
            f.extractall(CACHE)
        z.unlink()
    with open(root / "corpus.jsonl", encoding="utf-8") as f:
        corpus = {o["_id"]: (o.get("title", "") + " " + o["text"]).strip() for o in map(json.loads, f)}
    with open(root / "queries.jsonl", encoding="utf-8") as f:
        queries = {o["_id"]: o["text"] for o in map(json.loads, f)}
    qrels: dict[str, dict[str, int]] = {}
    with open(root / "qrels" / "test.tsv", encoding="utf-8") as f:
        next(f)
        for line in f:
            q, d, s = line.rstrip("\n").split("\t")
            if int(s) > 0:
                qrels.setdefault(q, {})[d] = int(s)
    return corpus, {q: queries[q] for q in qrels}, qrels


def per_query(run: dict[str, list[str]], qrels) -> dict[str, list[float]]:
    qs = list(qrels)
    return {
        "ndcg@10": [ndcg(run[q], qrels[q]) for q in qs],
        "recall@3": [recall(run[q], qrels[q], 3) for q in qs],
        "recall@10": [recall(run[q], qrels[q], 10) for q in qs],
    }


def evaluate(name: str, models: list[str]) -> dict:
    corpus, queries, qrels = load(name)
    ids, qids = list(corpus), list(qrels)
    print(f"== {name}: {len(ids)} docs, {len(qids)} queries", flush=True)
    runs, secs = {}, {}

    t0 = time.time()
    idx = BM25()
    for i in ids:
        idx.upsert(i, corpus[i])
    runs["bm25"] = {q: [d for d, _ in idx.search(queries[q], CANDIDATES)] for q in qids}
    secs["bm25"] = time.time() - t0

    for m in models:
        t0 = time.time()
        dv = np.asarray(embedding.embed_passages([corpus[i] for i in ids], m), dtype=np.float32)
        qv = np.asarray([embedding.embed_query(queries[q], m) for q in qids], dtype=np.float32)
        dv /= np.maximum(np.linalg.norm(dv, axis=1, keepdims=True), 1e-12)
        qv /= np.maximum(np.linalg.norm(qv, axis=1, keepdims=True), 1e-12)
        top = np.argsort(-(qv @ dv.T), axis=1)[:, :CANDIDATES]
        dense = {q: [ids[j] for j in row] for q, row in zip(qids, top)}
        runs[f"dense:{m}"] = dense
        secs[f"dense:{m}"] = time.time() - t0
        if m != "hash":
            runs[f"hybrid:{m}"] = {q: [d for d, _ in rrf(dense[q], runs["bm25"][q])] for q in qids}
        print(f"   encoded with {m} in {secs[f'dense:{m}']:.0f}s", flush=True)

    scores = {r: per_query(run, qrels) for r, run in runs.items()}
    out = {"docs": len(ids), "queries": len(qids), "seconds": secs,
           "mean": {r: {k: float(np.mean(v)) for k, v in s.items()} for r, s in scores.items()},
           "ci": {}}
    for m in models:
        h = f"hybrid:{m}"
        if h in scores:
            for other in ("bm25", f"dense:{m}"):
                out["ci"][f"{h} - {other}"] = paired_ci(scores[h]["ndcg@10"], scores[other]["ndcg@10"])
    return out


def to_markdown(name: str, res: dict, cfg: dict) -> str:
    lines = [f"# Retrieval eval: {name}", "",
             f"{cfg['date']} · BM25 k1={K1} b={B} · RRF k={RRF_K}, depth {CANDIDATES} · exact cosine", ""]
    for ds, r in res.items():
        lines += [f"## {ds} ({r['docs']} docs, {r['queries']} test queries)", "",
                  "| Method | nDCG@10 | Recall@3 | Recall@10 |", "|---|---|---|---|"]
        for run, m in r["mean"].items():
            lines.append(f"| {run} | " + " | ".join(f"{m[k]:.3f}" for k in METRICS) + " |")
        lines.append("")
        for pair, (lo, hi) in r["ci"].items():
            lines.append(f"- nDCG@10 `{pair}`: 95% CI [{lo:+.3f}, {hi:+.3f}]")
        lines.append("")
    return "\n".join(lines)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--datasets", default="scifact,nfcorpus")
    ap.add_argument("--models", default=f"hash,{embedding.DEFAULT_MODEL}")
    ap.add_argument("--out", help="写 <out>.json 和 <out>.md")
    a = ap.parse_args()
    models = a.models.split(",")
    cfg = {"date": datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"), "models": models,
           "bm25": {"k1": K1, "b": B}, "rrf_k": RRF_K, "candidates": CANDIDATES}
    res = {ds: evaluate(ds, models) for ds in a.datasets.split(",")}
    md = to_markdown(Path(a.out).name if a.out else "run", res, cfg)
    print(md)
    if a.out:
        Path(a.out + ".json").write_text(json.dumps({"config": cfg, "results": res}, indent=2) + "\n")
        Path(a.out + ".md").write_text(md + "\n")


if __name__ == "__main__":
    main()
