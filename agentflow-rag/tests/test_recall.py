"""Recall@K 检索质量指标(master 指标5),项目自己的小标注集:前 6 条和文档共享关键词,后 8 条是改写过的问法。

灌入标注集 → 每 query 取 topK → Recall@K = 命中期望 doc / 期望总数,对所有 query 取平均。
这是回归检查;方法之间的正式对比看 retrieval_eval.py(BEIR 公开数据集)。
"""
import json
import os

import psycopg
import pytest

from app import store

K = 3
DATA = os.path.join(os.path.dirname(__file__), "data", "labeled.json")


@pytest.fixture(scope="module")
def loaded(db):
    with open(DATA, encoding="utf-8") as f:
        data = json.load(f)
    store.init()
    # 用带前缀的 id 隔离,避免与其它数据混淆;跑完清理
    docs = [{"id": "lbl-" + d["id"], "text": d["text"]} for d in data["docs"]]
    store.upsert(docs)
    yield data
    with psycopg.connect(store.DB_URL) as conn, conn.cursor() as cur:
        cur.execute(f"DELETE FROM {store.TABLE} WHERE id LIKE 'lbl-%'")
        conn.commit()
    store.init()


def _mean_recall(data, mode):
    total = 0.0
    for q in data["queries"]:
        got = {h["id"].removeprefix("lbl-") for h in store.search(q["query"], K, mode)}
        total += len(got & set(q["expected"])) / len(q["expected"])
    return total / len(data["queries"])


def test_recall_at_k(loaded):
    recall = {m: _mean_recall(loaded, m) for m in ("bm25", "dense", "hybrid")}
    print(f"\n==> mean Recall@{K} over {len(loaded['queries'])} queries: "
          + ", ".join(f"{m}={r:.3f}" for m, r in recall.items()))
    assert recall["hybrid"] >= 0.9
