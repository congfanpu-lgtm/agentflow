#!/usr/bin/env bash
# 启动 RAG 服务(端口 8000)。前置:pgvector 容器在 5433。
set -euo pipefail
cd "$(dirname "$0")"
exec .venv/bin/uvicorn app.main:app --host 0.0.0.0 --port 8000
