# Retrieval eval: retrieval-v1

2026-10-17 23:26 UTC · BM25 k1=0.9 b=0.4 · RRF k=60, depth 100 · exact cosine

## scifact (5183 docs, 300 test queries)

| Method | nDCG@10 | Recall@3 | Recall@10 |
|---|---|---|---|
| bm25 | 0.681 | 0.696 | 0.806 |
| dense:hash | 0.211 | 0.204 | 0.293 |
| dense:BAAI/bge-small-en-v1.5 | 0.720 | 0.710 | 0.845 |
| hybrid:BAAI/bge-small-en-v1.5 | 0.733 | 0.720 | 0.856 |

- nDCG@10 `hybrid:BAAI/bge-small-en-v1.5 - bm25`: 95% CI [+0.028, +0.076]
- nDCG@10 `hybrid:BAAI/bge-small-en-v1.5 - dense:BAAI/bge-small-en-v1.5`: 95% CI [-0.012, +0.036]

## nfcorpus (3633 docs, 323 test queries)

| Method | nDCG@10 | Recall@3 | Recall@10 |
|---|---|---|---|
| bm25 | 0.322 | 0.103 | 0.153 |
| dense:hash | 0.074 | 0.018 | 0.037 |
| dense:BAAI/bge-small-en-v1.5 | 0.338 | 0.102 | 0.158 |
| hybrid:BAAI/bge-small-en-v1.5 | 0.365 | 0.115 | 0.174 |

- nDCG@10 `hybrid:BAAI/bge-small-en-v1.5 - bm25`: 95% CI [+0.030, +0.057]
- nDCG@10 `hybrid:BAAI/bge-small-en-v1.5 - dense:BAAI/bge-small-en-v1.5`: 95% CI [+0.014, +0.041]

