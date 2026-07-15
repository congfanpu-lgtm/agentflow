"""Recall@K 检索质量指标(master 指标5)。确定性 embedding 下可复现。

灌入标注集 → 每 query 取 topK → Recall@K = 命中期望 doc / 期望总数,对所有 query 取平均。
"""
import json
import os

import psycopg
import pytest

from app import store

K = 3
DATA = os.path.join(os.path.dirname(__file__), "data", "labeled.json")


@pytest.fixture(scope="module")
def loaded():
    with open(DATA, encoding="utf-8") as f:
        data = json.load(f)
    store.init()
    # 用带前缀的 id 隔离,避免与其它数据混淆;跑完清理
    docs = [{"id": "lbl-" + d["id"], "text": d["text"]} for d in data["docs"]]
    store.upsert(docs)
    yield data
    with psycopg.connect(store.DB_URL) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM documents WHERE id LIKE 'lbl-%'")
        conn.commit()


def test_recall_at_k(loaded):
    data = loaded
    total_recall = 0.0
    for q in data["queries"]:
        hits = store.search(q["query"], K)
        got = {h["id"].removeprefix("lbl-") for h in hits}
        expected = set(q["expected"])
        recall = len(got & expected) / len(expected)
        total_recall += recall
        print(f"Recall@{K} q='{q['query'][:40]}...' = {recall:.2f} (hits={sorted(got)})")
    mean_recall = total_recall / len(data["queries"])
    print(f"\n==> mean Recall@{K} = {mean_recall:.3f} over {len(data['queries'])} queries")
    # 确定性 embedding(词面相似)下的实测下限;真实 embedding 可更高(backlog)
    assert mean_recall >= 0.6
