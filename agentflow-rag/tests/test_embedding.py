import math

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
