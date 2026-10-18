from app.rank import BM25, rrf, tokens


def test_tokens_stem_and_drop_stopwords():
    assert tokens("The partitions are rebalanced") == ["partit", "rebalanc"]


def test_tokens_cjk_runs_become_bigrams():
    # 中文没有空格:连续汉字切成相邻二元组(Lucene CJKAnalyzer 的做法),单字保留
    assert tokens("分区重平衡") == ["分区", "区重", "重平", "平衡"]
    assert tokens("kafka 的 分区") == ["kafka", "的", "分区"]


def test_bm25_rare_term_outweighs_common_term():
    idx = BM25()
    idx.upsert("a", "rebalance")
    idx.upsert("b", "kafka kafka")
    idx.upsert("c", "kafka broker")
    # a 只有罕见词(idf 高),b 的常见词出现两次:没有 idf 时 b 会排第一
    assert [d for d, _ in idx.search("kafka rebalance", 3)][0] == "a"


def test_bm25_upsert_replaces_and_updates_stats():
    idx = BM25()
    idx.upsert("a", "redis lua script atomic single threaded")
    idx.upsert("b", "kafka partition")
    idx.upsert("a", "kafka partition rebalance")
    assert idx.search("redis", 5) == []
    fresh = BM25()   # 同样的最终内容直接建一份:替换后的统计量(文档数、总长度、df)应完全一致
    fresh.upsert("b", "kafka partition")
    fresh.upsert("a", "kafka partition rebalance")
    assert idx.search("kafka rebalance", 5) == fresh.search("kafka rebalance", 5)
    assert len(idx) == 2


def test_bm25_no_match_and_empty_index():
    assert BM25().search("anything", 3) == []
    idx = BM25()
    idx.upsert("a", "kafka")
    assert idx.search("banana", 3) == []


def test_rrf_rewards_agreement_between_lists():
    fused = [d for d, _ in rrf(["x", "a", "b"], ["y", "c", "a"])]
    assert fused[0] == "a"            # 两个列表都靠前,胜过各自的第一名
    assert set(fused) == {"x", "y", "a", "b", "c"}


def test_rrf_scores_only_depend_on_rank():
    # 原始分数量纲不同(BM25 ~10、余弦 ~0.8)也不用校准:只看名次
    assert rrf(["a"]) == rrf(["a"], [])
    assert rrf(["a", "b"])[0][1] == 1 / 61
