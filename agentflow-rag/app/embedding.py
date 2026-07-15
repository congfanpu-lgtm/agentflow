"""确定性 embedding:词哈希投影到固定维单位向量。

离线、可复现、无需 API key / 不下载模型——延续项目 mock-first 哲学(见 MockLlmClient)。
共享词越多余弦相似度越高,足以演示"相关文本召回"与 Recall@K,不卷复杂 RAG。
真实 embedding(sentence-transformers / OpenAI 兼容)是 backlog 的配置开关。

ponytail: 词面相似(bag-of-words 哈希),无深层语义;dim/是否 sublinear-tf/停用词为可校准常量。
"""
import hashlib
import math
import re

DIM = 256

_TOKEN = re.compile(r"[^a-z0-9]+")


def _tokens(text: str) -> list[str]:
    return [t for t in _TOKEN.split(text.lower()) if t]


def embed(text: str) -> list[float]:
    """文本 → DIM 维 L2 归一化向量。空文本返回零向量。"""
    vec = [0.0] * DIM
    for tok in _tokens(text):
        # 稳定哈希(内置 hash 跨进程加盐,不可复现)→ 用 md5 取整
        h = int(hashlib.md5(tok.encode("utf-8")).hexdigest(), 16)
        vec[h % DIM] += 1.0
    norm = math.sqrt(sum(v * v for v in vec))
    if norm == 0.0:
        return vec
    return [v / norm for v in vec]
