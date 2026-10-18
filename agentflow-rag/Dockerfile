FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
# 构建时把 embedding 模型下载进镜像(~70MB),运行时不联网
ENV RAG_EMBED_MODEL=BAAI/bge-small-en-v1.5 FASTEMBED_CACHE_PATH=/models
RUN python -c "import os; from fastembed import TextEmbedding; TextEmbedding(os.environ['RAG_EMBED_MODEL'])"
ENV HF_HUB_OFFLINE=1
COPY app app
EXPOSE 8000
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
