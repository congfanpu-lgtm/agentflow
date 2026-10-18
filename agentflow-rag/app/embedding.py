"""Embedding:默认用本地 ONNX 模型(fastembed,CPU 即可,无需 API key);"hash" 是旧的词哈希向量。

RAG_EMBED_MODEL 选模型。Docker 镜像构建时已把默认模型下载好,运行时不联网。
hash 只有词面相似(共享词越多越像),没有语义,留作离线兜底和评测里的对照组。
"""
import hashlib
import math
import os
import re
from functools import cache

DEFAULT_MODEL = "BAAI/bge-small-en-v1.5"
MODEL = os.environ.get("RAG_EMBED_MODEL", DEFAULT_MODEL)

DIM = 256
# fastembed 默认一批 256 条:长文档(512 token)时注意力矩阵占内存 ~9GB 而开始换页;32 条约 1GB
BATCH = 32

_TOKEN = re.compile(r"[^a-z0-9]+")


def _tokens(text: str) -> list[str]:
    return [t for t in _TOKEN.split(text.lower()) if t]


def embed(text: str) -> list[float]:
    """词哈希:文本 → DIM 维 L2 归一化向量。空文本返回零向量。"""
    vec = [0.0] * DIM
    for tok in _tokens(text):
        # 稳定哈希(内置 hash 跨进程加盐,不可复现)→ 用 md5 取整
        h = int(hashlib.md5(tok.encode("utf-8")).hexdigest(), 16)
        vec[h % DIM] += 1.0
    norm = math.sqrt(sum(v * v for v in vec))
    if norm == 0.0:
        return vec
    return [v / norm for v in vec]


@cache
def _load(model: str):
    from fastembed import TextEmbedding   # 延迟导入:只用 hash 时不加载 onnxruntime
    return TextEmbedding(model)


def embed_passages(texts: list[str], model: str = MODEL) -> list[list[float]]:
    """入库文档的向量。"""
    if model == "hash":
        return [embed(t) for t in texts]
    return [v.tolist() for v in _load(model).passage_embed(texts, batch_size=BATCH)]


def embed_query(text: str, model: str = MODEL) -> list[float]:
    """查询的向量(部分模型对查询和文档用不同前缀,所以分开)。"""
    if model == "hash":
        return embed(text)
    return next(iter(_load(model).query_embed([text]))).tolist()


@cache
def dim(model: str = MODEL) -> int:
    return len(embed_query("dim", model))
