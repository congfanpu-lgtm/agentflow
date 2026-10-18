import math

import numpy as np
import pytest

from retrieval_eval import ndcg, paired_ci, recall


def test_ndcg_rank_discount():
    assert ndcg(["a", "b"], {"a": 1}) == 1
    assert ndcg(["x", "a"], {"a": 1}) == pytest.approx(1 / math.log2(3))
    assert ndcg(["x", "y"], {"a": 1}) == 0


def test_ndcg_uses_linear_gain_like_beir():
    # BEIR 用 pytrec_eval(trec_eval 的 ndcg_cut):增益 = 相关度本身,不是 2^rel - 1
    got = ndcg(["b", "a"], {"a": 2, "b": 1})
    assert got == pytest.approx((1 + 2 / math.log2(3)) / (2 + 1 / math.log2(3)))


def test_recall_counts_relevant_found_in_top_k():
    assert recall(["a", "x", "b"], {"a": 1, "b": 1, "c": 1}, 2) == pytest.approx(1 / 3)
    assert recall(["a", "x", "b"], {"a": 1, "b": 1, "c": 1}, 3) == pytest.approx(2 / 3)


def test_paired_ci():
    lo, hi = paired_ci([1, 0] * 50, [0, 1] * 50)      # 一半赢一半输:区间应跨过 0
    assert lo < 0 < hi
    # 每个查询都稳定好 0.01,但查询之间差异很大:配对后区间很窄;不配对的话会宽到跨过 0
    b = np.random.default_rng(1).random(300)
    lo, hi = paired_ci(b + 0.01, b)
    assert lo == pytest.approx(0.01) and hi == pytest.approx(0.01)
