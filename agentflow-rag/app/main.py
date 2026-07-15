"""AgentFlow RAG 检索微服务(Python/FastAPI)。

Agent 的检索工具:/rag/ingest 灌库、/rag/search 近邻检索。跨语言微服务(Java 主链路 + Python
embedding 生态),对应 master 设计的 polyglot 架构与"意图驱动检索工具"。
"""
from contextlib import asynccontextmanager

from fastapi import FastAPI
from pydantic import BaseModel

from . import store


class Doc(BaseModel):
    id: str
    text: str


class IngestRequest(BaseModel):
    docs: list[Doc]


class SearchRequest(BaseModel):
    query: str
    topK: int = 3


@asynccontextmanager
async def lifespan(_: FastAPI):
    store.init()
    yield


app = FastAPI(title="AgentFlow RAG", lifespan=lifespan)


@app.get("/health")
def health():
    return {"status": "ok"}


@app.post("/rag/ingest")
def ingest(req: IngestRequest):
    n = store.upsert([d.model_dump() for d in req.docs])
    return {"ingested": n}


@app.post("/rag/search")
def search(req: SearchRequest):
    return {"hits": store.search(req.query, req.topK)}
