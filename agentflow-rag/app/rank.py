"""排序原语:BM25 关键词索引 + RRF 融合。服务和离线评测(retrieval_eval.py)共用这一份。

为什么要关键词检索:语义向量擅长同义改写,但对专有名词、版本号、错误码这类"字面必须一致"的查询
反而不如 BM25;两路各取前 CANDIDATES 名,用 RRF 按名次融合,不需要校准两边分数的量纲。
"""
import heapq
import math
import re
import threading
from collections import Counter, defaultdict

from py_rust_stemmers import SnowballStemmer

K1, B = 0.9, 0.4        # BEIR 论文里 BM25 基线(Anserini)用的参数
RRF_K = 60              # RRF 原论文(Cormack et al., 2009)的常数
CANDIDATES = 100        # 混合检索时每一路取多少名参与融合

_WORD = re.compile(r"[a-z0-9]+|[㐀-鿿]+")
# Lucene / Elasticsearch 默认英文停用词
_STOP = frozenset(
    "a an and are as at be but by for if in into is it no not of on or such that the their then "
    "there these they this to was will with".split()
)
_STEM = SnowballStemmer("english")


def tokens(text: str) -> list[str]:
    """英文:小写 + 去停用词 + Snowball 词干;连续汉字:相邻二元组(单字保留)。"""
    out = []
    for m in _WORD.findall(text.lower()):
        if m[0] >= "㐀":
            out += [m[i:i + 2] for i in range(len(m) - 1)] or [m]
        elif m not in _STOP:
            out.append(_STEM.stem_word(m))
    return out


class BM25:
    """Okapi BM25,内存倒排索引。同一个 id 再次 upsert 会替换旧文档(统计量一起更新)。

    ponytail: 每个进程一份内存索引,启动时从 Postgres 重建;单副本、十万级短文档够用。
    多副本或语料更大时,改用库内 BM25(如 ParadeDB pg_search)。
    """

    def __init__(self):
        self._lock = threading.Lock()
        self._tf: dict[str, Counter] = {}
        self._len: dict[str, int] = {}
        self._post: dict[str, dict[str, int]] = defaultdict(dict)   # term -> {doc id: tf}
        self._total = 0

    def __len__(self) -> int:
        return len(self._tf)

    def upsert(self, doc_id: str, text: str) -> None:
        tf = Counter(tokens(text))
        with self._lock:
            self._remove(doc_id)
            self._tf[doc_id] = tf
            self._len[doc_id] = n = sum(tf.values())
            self._total += n
            for t, f in tf.items():
                self._post[t][doc_id] = f

    def _remove(self, doc_id: str) -> None:
        tf = self._tf.pop(doc_id, None)
        if tf is None:
            return
        self._total -= self._len.pop(doc_id)
        for t in tf:
            del self._post[t][doc_id]
            if not self._post[t]:
                del self._post[t]

    def search(self, query: str, k: int) -> list[tuple[str, float]]:
        """返回 [(id, score)],按分数降序;没有任何词命中的文档不返回。"""
        with self._lock:
            n = len(self._tf)
            if n == 0:
                return []
            avg = self._total / n or 1.0
            scores: dict[str, float] = defaultdict(float)
            for t in set(tokens(query)):
                post = self._post.get(t)
                if not post:
                    continue
                idf = math.log(1 + (n - len(post) + 0.5) / (len(post) + 0.5))
                for d, f in post.items():
                    scores[d] += idf * f * (K1 + 1) / (f + K1 * (1 - B + B * self._len[d] / avg))
            return heapq.nlargest(k, scores.items(), key=lambda x: x[1])


def rrf(*ranked: list[str]) -> list[tuple[str, float]]:
    """Reciprocal Rank Fusion:score(d) = Σ 1/(RRF_K + 名次)。只用名次,所以不同检索器的分数不用校准。"""
    scores: dict[str, float] = defaultdict(float)
    for ids in ranked:
        for r, d in enumerate(ids, 1):
            scores[d] += 1 / (RRF_K + r)
    return sorted(scores.items(), key=lambda x: -x[1])
