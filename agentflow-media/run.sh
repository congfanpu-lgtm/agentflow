#!/usr/bin/env bash
# 启动媒体服务。默认只监听本机(MEDIA_HOST 可改成容器网关地址,让本机的容器也能访问);必须单进程(GPU 锁和计数器在进程内)。
set -euo pipefail
cd "$(dirname "$0")"
NV=$(echo .venv/lib/python3.*/site-packages/nvidia)
if [ -d "$NV/cublas/lib" ]; then   # 用 pip 装的 CUDA 库时,ctranslate2 要靠这个找到它们
  export LD_LIBRARY_PATH="$PWD/$NV/cublas/lib:$PWD/$NV/cudnn/lib:/usr/lib/wsl/lib:${LD_LIBRARY_PATH:-}"
fi
exec .venv/bin/uvicorn app.main:app --host "${MEDIA_HOST:-127.0.0.1}" --port "${MEDIA_PORT:-8010}" --workers 1
