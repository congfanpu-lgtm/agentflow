"""pgvector 存储 + 检索。三种方式:dense(pgvector 余弦)、bm25(内存倒排)、hybrid(两路 RRF 融合,默认)。

表 documents(id, content, embedding vector(D)),D 由当前 embedding 模型决定,模型名记在 embedding 列的注释里。
换了模型(只比维度不够:同维度的两个模型向量不可比)时,init 在同一个事务里把所有文档按新模型重新编码:
内容不丢,失败则回滚到旧表。
演示规模用精确检索(无 ivfflat/hnsw 索引);索引/分片留 backlog。
"""
import logging
import os
import threading

import psycopg
from psycopg import sql

from . import embedding
from .rank import BM25, CANDIDATES, rrf

DB_URL = os.environ.get(
    "RAG_DB_URL", "postgresql://postgres:agentflow@localhost:5433/agentflow_rag"
)
TABLE = "documents"

log = logging.getLogger(__name__)
_bm25 = BM25()
_write = threading.Lock()   # 写库和更新 BM25 作为一步:同一 id 并发写入时,索引不会停在旧版本


def _vec_literal(vec: list[float]) -> str:
    return "[" + ",".join(repr(x) for x in vec) + "]"


def init() -> None:
    """建扩展/建表;模型或维度和库里记录的不符时重新编码;再从库里重建 BM25 索引。"""
    global _bm25
    d = embedding.dim()
    with psycopg.connect(DB_URL) as conn, conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS {TABLE} (
                    id text PRIMARY KEY,
                    content text NOT NULL,
                    embedding vector({d}) NOT NULL
                )"""
        )
        cur.execute(
            "SELECT atttypmod, col_description(attrelid, attnum) FROM pg_attribute"
            " WHERE attrelid = %s::regclass AND attname = 'embedding'",
            (TABLE,),
        )
        if cur.fetchone() != (d, embedding.MODEL):
            _reembed(cur, d)
            cur.execute(sql.SQL("COMMENT ON COLUMN {}.embedding IS {}").format(
                sql.Identifier(TABLE), sql.Literal(embedding.MODEL)))
        cur.execute(f"SELECT id, content FROM {TABLE}")
        rows = cur.fetchall()
        conn.commit()
    idx = BM25()
    for doc_id, content in rows:
        idx.upsert(doc_id, content)
    _bm25 = idx


def _reembed(cur, d: int) -> None:
    cur.execute(f"ALTER TABLE {TABLE} DROP COLUMN embedding, ADD COLUMN embedding vector({d})")
    cur.execute(f"SELECT id, content FROM {TABLE}")
    rows = cur.fetchall()
    log.warning("embedding 模型变为 %s(%d 维),重新编码 %d 篇文档", embedding.MODEL, d, len(rows))
    vecs = embedding.embed_passages([c for _, c in rows])
    cur.executemany(
        f"UPDATE {TABLE} SET embedding = %s::vector WHERE id = %s",
        [(_vec_literal(v), doc_id) for v, (doc_id, _) in zip(vecs, rows)],
    )
    cur.execute(f"ALTER TABLE {TABLE} ALTER COLUMN embedding SET NOT NULL")


def upsert(docs: list[dict]) -> int:
    """docs: [{"id","text"}]。id 冲突则覆盖。返回写入条数。"""
    vecs = embedding.embed_passages([d["text"] for d in docs])
    # ponytail: 全局写锁让所有 ingest 串行;写入量大时改成按 id 分段加锁
    with _write, psycopg.connect(DB_URL) as conn, conn.cursor() as cur:
        cur.executemany(
            f"""INSERT INTO {TABLE} (id, content, embedding)
               VALUES (%s, %s, %s::vector)
               ON CONFLICT (id) DO UPDATE
                 SET content = EXCLUDED.content, embedding = EXCLUDED.embedding""",
            [(d["id"], d["text"], _vec_literal(v)) for d, v in zip(docs, vecs)],
        )
        conn.commit()
        for d in docs:   # 提交后再进索引,BM25 不会领先于库
            _bm25.upsert(d["id"], d["text"])
    return len(docs)


def search(query: str, k: int, mode: str = "hybrid") -> list[dict]:
    """返回按分数降序的 top-k:[{"id","text","score"}]。score 的含义随 mode:余弦 / BM25 / RRF。"""
    with psycopg.connect(DB_URL) as conn, conn.cursor() as cur:
        if mode == "bm25":
            ranked = _bm25.search(query, CANDIDATES)
        elif mode == "dense":
            ranked = _dense(cur, query, k)
        else:
            dense = [d for d, _ in _dense(cur, query, CANDIDATES)]
            lexical = [d for d, _ in _bm25.search(query, CANDIDATES)]
            ranked = rrf(dense, lexical)
        cur.execute(f"SELECT id, content FROM {TABLE} WHERE id = ANY(%s)", ([d for d, _ in ranked],))
        text = dict(cur.fetchall())
    # 先过滤再截断:库里已删、内存索引还记着的 id 不占名额
    return [{"id": d, "text": text[d], "score": float(s)} for d, s in ranked if d in text][:k]


def _dense(cur, query: str, n: int) -> list[tuple[str, float]]:
    q = _vec_literal(embedding.embed_query(query))
    cur.execute(
        f"""SELECT id, 1 - (embedding <=> %s::vector) AS score
           FROM {TABLE}
           ORDER BY embedding <=> %s::vector
           LIMIT %s""",
        (q, q, n),
    )
    return cur.fetchall()
