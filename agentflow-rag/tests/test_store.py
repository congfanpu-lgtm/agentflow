"""store 集成测试(真 pgvector,端口 5433,独立测试库,见 conftest)。"""
import uuid

import psycopg
import pytest

from app import embedding, store

KAFKA = "Kafka consumer group rebalance reassigns partitions when workers join or leave"
RATE = "Token bucket rate limiting throttles clients that exceed their quota"
BANANA = "Banana smoothie recipe with yogurt and honey"
PARAPHRASE = "how to stop a service from being overwhelmed by too many calls"   # 和 RATE 没有共同词


@pytest.fixture
def docs(db):
    store.init()
    p = f"t{uuid.uuid4().hex[:8]}-"
    store.upsert([{"id": p + "kafka", "text": KAFKA}, {"id": p + "rate", "text": RATE},
                  {"id": p + "banana", "text": BANANA}])
    yield p
    with psycopg.connect(store.DB_URL) as conn, conn.cursor() as cur:
        cur.execute(f"DELETE FROM {store.TABLE} WHERE id LIKE %s", (p + "%",))
        conn.commit()


def _mine(p, hits):
    """库里可能还有别的文档,只看本测试写入的那几篇的相对顺序。"""
    return [h["id"].removeprefix(p) for h in hits if h["id"].startswith(p)]


def test_hybrid_ranks_closest_first(docs):
    hits = store.search("kafka partition rebalance", k=10)
    assert _mine(docs, hits)[0] == "kafka"
    assert [h["score"] for h in hits] == sorted((h["score"] for h in hits), reverse=True)
    assert all(h["text"] for h in hits)


def test_dense_finds_paraphrase_that_keywords_miss(docs):
    assert "rate" not in _mine(docs, store.search(PARAPHRASE, k=10, mode="bm25"))
    assert _mine(docs, store.search(PARAPHRASE, k=10, mode="dense"))[0] == "rate"
    assert _mine(docs, store.search(PARAPHRASE, k=10))[0] == "rate"      # 混合检索不丢语义命中


def test_bm25_is_updated_on_upsert_without_reinit(docs):
    assert _mine(docs, store.search("rebalance", k=10, mode="bm25")) == ["kafka"]


@pytest.mark.parametrize("mode", ["bm25", "hybrid"])
def test_ids_missing_from_db_do_not_use_up_top_k(docs, mode):
    """库里被直接删掉、但内存 BM25 还记着的文档:先过滤再截断,不能少返回。"""
    store.upsert([{"id": docs + "k2", "text": "kafka kafka kafka"}, {"id": docs + "k3", "text": "kafka broker"}])
    with psycopg.connect(store.DB_URL) as conn:
        conn.execute(f"DELETE FROM {store.TABLE} WHERE id = %s", (docs + "k2",))
    hits = _mine(docs, store.search("kafka", k=2, mode=mode))
    assert "k2" not in hits and len(hits) == 2


def _column(table):
    with psycopg.connect(store.DB_URL) as conn:
        return conn.execute(
            "SELECT atttypmod, attnotnull, col_description(attrelid, attnum) FROM pg_attribute"
            " WHERE attrelid = %s::regclass AND attname = 'embedding'", (table,)).fetchone()


@pytest.fixture
def old_table(db, monkeypatch):
    """建一张"旧"表(给定维度 + 列注释记录的模型名),写两篇向量全相同的文档,init 后看是否重新编码。"""
    table = f"documents_mig_{uuid.uuid4().hex[:8]}"
    monkeypatch.setattr(store, "TABLE", table)

    def make(dim, model):
        with psycopg.connect(store.DB_URL) as conn:
            conn.execute(f"CREATE TABLE {table} (id text PRIMARY KEY, content text NOT NULL,"
                         f" embedding vector({dim}) NOT NULL)")
            if model:
                conn.execute(f"COMMENT ON COLUMN {table}.embedding IS '{model}'")
            same = store._vec_literal([1.0] + [0.0] * (dim - 1))
            for doc_id, text in (("kafka", KAFKA), ("rate", RATE)):
                conn.execute(f"INSERT INTO {table} VALUES (%s, %s, %s::vector)", (doc_id, text, same))
        return table

    yield make
    with psycopg.connect(store.DB_URL) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {table}")
    monkeypatch.undo()
    store.init()


@pytest.mark.parametrize("dim, model", [
    (embedding.DIM, None),                   # 旧版本的 256 维 hash 表,没有记录模型名
    (embedding.dim(), "some/other-model"),   # 维度相同但模型不同:只比维度会漏掉,新旧向量混在一起
])
def test_init_reencodes_when_model_changes(old_table, dim, model):
    table = old_table(dim, model)
    store.init()
    assert _column(table) == (embedding.dim(), True, embedding.MODEL)
    assert [h["id"] for h in store.search(PARAPHRASE, k=2, mode="dense")] == ["rate", "kafka"]
    assert [h["id"] for h in store.search("kafka rebalance", k=1, mode="bm25")] == ["kafka"]


def test_init_leaves_matching_table_alone(old_table):
    """模型没变时不重新编码(这里存的是假向量,如果被重新编码,dense 的顺序会变成正确的)。"""
    table = old_table(embedding.dim(), embedding.MODEL)
    store.init()
    assert _column(table) == (embedding.dim(), True, embedding.MODEL)
    scores = {h["id"]: h["score"] for h in store.search(PARAPHRASE, k=2, mode="dense")}
    assert scores["rate"] == scores["kafka"]
