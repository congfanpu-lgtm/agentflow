"""测试用独立的库 agentflow_rag_test,不碰本地正在跑的 RAG 服务的数据。

两边共用一个库时,用不同模型跑测试(比如 RAG_EMBED_MODEL=hash)会把服务的向量列整个改写掉。
store 在导入时读 RAG_DB_URL,所以要在测试模块导入 app 之前设好。
"""
import os

import psycopg
import pytest

TEST_DB = "agentflow_rag_test"
os.environ.setdefault("RAG_DB_URL", f"postgresql://postgres:agentflow@localhost:5433/{TEST_DB}")


@pytest.fixture(scope="session")
def db():
    """需要 Postgres 的测试才用;纯逻辑的测试不连库。"""
    url = os.environ["RAG_DB_URL"]
    with psycopg.connect(url.rsplit("/", 1)[0] + "/postgres", autocommit=True) as conn:
        name = url.rsplit("/", 1)[1]
        if not conn.execute("SELECT 1 FROM pg_database WHERE datname = %s", (name,)).fetchone():
            conn.execute(f'CREATE DATABASE "{name}"')
    return url
