"""pgvector 存储:建表/建扩展、upsert、余弦 ANN 检索。

表 documents(id, content, embedding vector(256))。检索用 `<=>`(余弦距离),score = 1 - 距离。
演示规模用精确检索(无 ivfflat 索引);索引/分片留 backlog。
"""
import os

import psycopg

from .embedding import DIM, embed

DB_URL = os.environ.get(
    "RAG_DB_URL", "postgresql://postgres:agentflow@localhost:5433/agentflow_rag"
)


def _vec_literal(vec: list[float]) -> str:
    return "[" + ",".join(repr(x) for x in vec) + "]"


def init() -> None:
    with psycopg.connect(DB_URL) as conn, conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS documents (
                    id text PRIMARY KEY,
                    content text NOT NULL,
                    embedding vector({DIM}) NOT NULL
                )"""
        )
        conn.commit()


def upsert(docs: list[dict]) -> int:
    """docs: [{"id","text"}]。id 冲突则覆盖。返回写入条数。"""
    with psycopg.connect(DB_URL) as conn, conn.cursor() as cur:
        for d in docs:
            cur.execute(
                """INSERT INTO documents (id, content, embedding)
                   VALUES (%s, %s, %s::vector)
                   ON CONFLICT (id) DO UPDATE
                     SET content = EXCLUDED.content, embedding = EXCLUDED.embedding""",
                (d["id"], d["text"], _vec_literal(embed(d["text"]))),
            )
        conn.commit()
    return len(docs)


def search(query: str, k: int) -> list[dict]:
    """返回按余弦相似降序的 top-k:[{"id","text","score"}]。"""
    qvec = _vec_literal(embed(query))
    with psycopg.connect(DB_URL) as conn, conn.cursor() as cur:
        cur.execute(
            """SELECT id, content, 1 - (embedding <=> %s::vector) AS score
               FROM documents
               ORDER BY embedding <=> %s::vector
               LIMIT %s""",
            (qvec, qvec, k),
        )
        return [{"id": r[0], "text": r[1], "score": float(r[2])} for r in cur.fetchall()]
