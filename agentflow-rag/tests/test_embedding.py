import math

import numpy as np

from app import embedding
from app.embedding import DIM, embed


def test_dim_and_unit_norm():
    v = embed("distributed systems idempotency")
    assert len(v) == DIM
    assert abs(math.sqrt(sum(x * x for x in v)) - 1.0) < 1e-9


def test_empty_is_zero_vector():
    v = embed("")
    assert v == [0.0] * DIM


def test_deterministic():
    assert embed("token bucket rate limit") == embed("token bucket rate limit")


def _cos(a, b):
    return sum(x * y for x, y in zip(a, b))


def test_shared_words_more_similar_than_unrelated():
    base = embed("kafka message queue partition")
    related = embed("kafka partition rebalance consumer")   # 共享 kafka/partition
    unrelated = embed("banana smoothie recipe kitchen")
    assert _cos(base, related) > _cos(base, unrelated)


# 下面是默认的真实模型(RAG_EMBED_MODEL)

QUERY = "how to stop a service from being overwhelmed by too many calls"
RELATED = "Token bucket rate limiting throttles clients that exceed their quota"
UNRELATED = "Banana smoothie recipe with yogurt and honey"


def test_real_model_matches_meaning_without_shared_words():
    # 和两篇都没有一个共同词:靠词面相似(hash / BM25)分不出来,真实模型应能分出哪篇意思相近
    words = set(QUERY.lower().split())
    assert not words & set(RELATED.lower().split()) and not words & set(UNRELATED.lower().split())
    q = embedding.embed_query(QUERY)
    rel, unrel = embedding.embed_passages([RELATED, UNRELATED])
    assert _cos(q, rel) > _cos(q, unrel) + 0.1


def test_vectors_are_plain_float_lists_of_model_dim():
    q = embedding.embed_query("kafka")
    (p,) = embedding.embed_passages(["kafka"])
    assert len(q) == len(p) == embedding.dim()
    assert type(q[0]) is float                          # 拼 pgvector 字面量时不能是 np.float32
    assert abs(np.linalg.norm(q) - 1.0) < 1e-3


def test_hash_model_is_selectable_offline():
    assert embedding.embed_query("kafka", "hash") == embed("kafka")
    assert embedding.dim("hash") == DIM
