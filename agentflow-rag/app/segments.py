"""课程录播的片段:一讲课切成的语音片段和幻灯片文字,带课程、讲次和时间,检索按课程隔离。

和 documents 表分开:那张表是研究 agent 的知识库,混在一起两边的检索都会被对方的内容干扰。
检索方式和 store 一样(dense / bm25 / 两路 RRF 融合),只是每一路都限定在一门课里。
片段是能重新算出来的(逐字稿在媒体服务里),所以一讲的写入是整讲替换,重复执行不会留下两份。
"""
import threading

import psycopg

from . import embedding, store
from .rank import BM25, CANDIDATES, rrf

TABLE = "segments"

_bm25: dict[int, BM25] = {}      # 课程 id → 这门课的词法索引
_write = threading.Lock()        # 写库和重建那门课的 BM25 作为一步


def init() -> None:
    """建表,向量列和当前模型对齐,从库里按课程重建 BM25。"""
    global _bm25
    d = embedding.dim()
    with psycopg.connect(store.DB_URL) as conn, conn.cursor() as cur:
        cur.execute("CREATE EXTENSION IF NOT EXISTS vector")
        cur.execute(
            f"""CREATE TABLE IF NOT EXISTS {TABLE} (
                    id text PRIMARY KEY,
                    course_id bigint NOT NULL,
                    lecture_id bigint NOT NULL,
                    start_ms bigint NOT NULL,
                    end_ms bigint NOT NULL,
                    kind text NOT NULL,
                    content text NOT NULL,
                    embedding vector({d}) NOT NULL
                )"""
        )
        cur.execute(f"CREATE INDEX IF NOT EXISTS segments_course_lecture ON {TABLE} (course_id, lecture_id)")
        store.sync_embedding(cur, TABLE, d)
        cur.execute(f"SELECT course_id, id, content FROM {TABLE}")
        rows = cur.fetchall()
        conn.commit()
    fresh: dict[int, BM25] = {}
    for course_id, seg_id, content in rows:
        fresh.setdefault(course_id, BM25()).upsert(seg_id, content)
    _bm25 = fresh


def index(course_id: int, lecture_id: int, segs: list[dict]) -> int:
    """整讲替换:segs 是 [{"startMs","endMs","kind","text"}];空列表等于清空这一讲。返回写入条数。"""
    vecs = embedding.embed_passages([s["text"] for s in segs]) if segs else []
    rows = [(f"{lecture_id}:{s['kind']}:{s['startMs']}", course_id, lecture_id, s["startMs"], s["endMs"],
             s["kind"], s["text"], store._vec_literal(v)) for s, v in zip(segs, vecs)]
    with _write, psycopg.connect(store.DB_URL) as conn, conn.cursor() as cur:
        # 讲次 id 全局唯一,所以按讲次删就够了;删和写在一个事务里,检索不会看到半讲
        cur.execute(f"DELETE FROM {TABLE} WHERE lecture_id = %s", (lecture_id,))
        cur.executemany(
            f"""INSERT INTO {TABLE} (id, course_id, lecture_id, start_ms, end_ms, kind, content, embedding)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s::vector)
                ON CONFLICT (id) DO UPDATE
                  SET end_ms = EXCLUDED.end_ms, content = EXCLUDED.content, embedding = EXCLUDED.embedding""",
            rows,
        )
        cur.execute(f"SELECT id, content FROM {TABLE} WHERE course_id = %s", (course_id,))
        remaining = cur.fetchall()
        conn.commit()
        idx = BM25()   # 提交后按库里的内容重建这门课的索引:旧片段不会留在词法索引里
        for seg_id, content in remaining:
            idx.upsert(seg_id, content)
        _bm25[course_id] = idx
    return len(rows)


def search(course_id: int, query: str, k: int, lecture_ids: list[int] | None = None,
           mode: str = "hybrid") -> list[dict]:
    """这门课里按分数降序的 top-k:[{"id","lectureId","startMs","endMs","kind","text","score"}]。"""
    with psycopg.connect(store.DB_URL) as conn, conn.cursor() as cur:
        if mode == "bm25":
            ranked = _lexical(course_id, query, lecture_ids)
        elif mode == "dense":
            ranked = _dense(cur, course_id, query, k, lecture_ids)
        else:
            dense = [d for d, _ in _dense(cur, course_id, query, CANDIDATES, lecture_ids)]
            ranked = rrf(dense, [d for d, _ in _lexical(course_id, query, lecture_ids)])
        # 再按课程查一遍正文:内存索引里即使有别的课或已删掉的片段,也出不去
        cur.execute(
            f"SELECT id, lecture_id, start_ms, end_ms, kind, content FROM {TABLE} WHERE course_id = %s AND id = ANY(%s)",
            (course_id, [d for d, _ in ranked]),
        )
        rows = {r[0]: r for r in cur.fetchall()}
    return [{"id": d, "lectureId": rows[d][1], "startMs": rows[d][2], "endMs": rows[d][3], "kind": rows[d][4],
             "text": rows[d][5], "score": float(s)} for d, s in ranked if d in rows][:k]


def _lexical(course_id: int, query: str, lecture_ids: list[int] | None) -> list[tuple[str, float]]:
    idx = _bm25.get(course_id)
    if idx is None:
        return []
    hits = idx.search(query, CANDIDATES)
    if lecture_ids is not None:
        # ponytail: 讲次过滤在全课前 100 个候选之后做,课很大、过滤很窄时词法候选会不足 k 个;真遇到再给每讲建索引
        wanted = {str(i) for i in lecture_ids}
        hits = [(d, s) for d, s in hits if d.split(":", 1)[0] in wanted]
    return hits


def _dense(cur, course_id: int, query: str, n: int, lecture_ids: list[int] | None) -> list[tuple[str, float]]:
    q = store._vec_literal(embedding.embed_query(query))
    cur.execute(
        f"""SELECT id, 1 - (embedding <=> %s::vector) AS score
            FROM {TABLE}
            WHERE course_id = %s AND (%s::bigint[] IS NULL OR lecture_id = ANY(%s::bigint[]))
            ORDER BY embedding <=> %s::vector
            LIMIT %s""",
        (q, course_id, lecture_ids, lecture_ids, q, n),
    )
    return cur.fetchall()
