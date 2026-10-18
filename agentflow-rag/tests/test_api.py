from fastapi.testclient import TestClient

from app.main import app
from app.rank import CANDIDATES

client = TestClient(app)   # 不进 with:不跑 lifespan,不连库;这里只测请求校验


def test_top_k_must_be_in_range():
    for k in (0, -1, CANDIDATES + 1):
        assert client.post("/rag/search", json={"query": "x", "topK": k}).status_code == 422


def test_unknown_mode_rejected():
    assert client.post("/rag/search", json={"query": "x", "mode": "nope"}).status_code == 422
