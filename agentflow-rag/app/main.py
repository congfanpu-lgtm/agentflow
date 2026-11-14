"""AgentFlow RAG 检索微服务(Python/FastAPI)。

Agent 的检索工具:/rag/ingest 灌库、/rag/search 检索(默认 BM25 + 语义向量混合)。跨语言微服务(Java 主链路 + Python
embedding 生态),对应 master 设计的 polyglot 架构与"意图驱动检索工具"。
"""
from contextlib import asynccontextmanager
from typing import Literal

from fastapi import FastAPI
from pydantic import BaseModel, Field

from . import embedding, segments, store
from .rank import CANDIDATES


class Doc(BaseModel):
    id: str
    text: str


class IngestRequest(BaseModel):
    docs: list[Doc]


class SearchRequest(BaseModel):
    query: str
    topK: int = Field(3, ge=1, le=CANDIDATES)
    mode: Literal["hybrid", "dense", "bm25"] = "hybrid"


class Segment(BaseModel):
    startMs: int = Field(ge=0)
    endMs: int = Field(ge=0)
    kind: Literal["speech", "slide"]
    text: str = Field(min_length=1)


class IndexSegmentsRequest(BaseModel):
    courseId: int
    lectureId: int
    segments: list[Segment]


class SearchSegmentsRequest(BaseModel):
    courseId: int
    query: str
    topK: int = Field(5, ge=1, le=50)
    lectureIds: list[int] | None = None
    mode: Literal["hybrid", "dense", "bm25"] = "hybrid"


@asynccontextmanager
async def lifespan(_: FastAPI):
    store.init()
    segments.init()
    yield


app = FastAPI(title="AgentFlow RAG", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok", "model": embedding.MODEL}


@app.post("/rag/ingest")
def ingest(req: IngestRequest):
    n = store.upsert([d.model_dump() for d in req.docs])
    return {"ingested": n}


@app.post("/rag/search")
def search(req: SearchRequest):
    return {"hits": store.search(req.query, req.topK, req.mode)}


@app.post("/segments/index")
def index_segments(req: IndexSegmentsRequest):
    """课程录播:整讲替换这一讲的片段(重复调用不会留下两份)。"""
    n = segments.index(req.courseId, req.lectureId, [s.model_dump() for s in req.segments])
    return {"indexed": n}


@app.post("/segments/search")
def search_segments(req: SearchSegmentsRequest):
    """课程录播:只在这门课里检索,可以再限定到几讲。"""
    return {"hits": segments.search(req.courseId, req.query, req.topK, req.lectureIds, req.mode)}
