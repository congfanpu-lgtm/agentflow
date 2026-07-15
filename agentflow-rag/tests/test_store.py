"""store 集成测试(真 pgvector,端口 5433)。"""
import uuid

from app import store


def test_upsert_then_search_ranks_closest_first():
    store.init()
    p = f"t{uuid.uuid4().hex[:8]}-"
    docs = [
        {"id": p + "kafka", "text": "kafka message queue partition consumer group"},
        {"id": p + "redis", "text": "redis token bucket rate limit lua atomic"},
        {"id": p + "banana", "text": "banana smoothie recipe kitchen fruit"},
    ]
    assert store.upsert(docs) == 3

    hits = store.search("kafka partition rebalance", k=3)
    ids = [h["id"] for h in hits]
    assert ids[0] == p + "kafka"          # 最相关排第一
    assert hits[0]["score"] >= hits[-1]["score"]   # 降序

    # 清理
    import psycopg
    with psycopg.connect(store.DB_URL) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM documents WHERE id LIKE %s", (p + "%",))
        conn.commit()
