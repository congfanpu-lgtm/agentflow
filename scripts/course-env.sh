#!/usr/bin/env bash
# 课程录播的两套隔离环境(都叠加 docker/course.yml,都固定用模拟模型):
#   agentflow-coursetest  只有中间件,给测试用(测试和在跑的 server/worker 共用 Kafka 会互相抢消息)
#   agentflow-course      整套服务,server 在 127.0.0.1:18090
# 在有 Docker 的机器上运行。媒体服务(agentflow-media)要另外启动,并且监听容器能访问到的地址,见 README。
set -euo pipefail
cd "$(dirname "$0")/.."
T() { docker compose -p agentflow-coursetest -f docker-compose.yml -f docker/course.yml "$@"; }
C() { docker compose -p agentflow-course -f docker-compose.yml -f docker/course.yml "$@"; }
ME="$(id -u):$(id -g)"
case "${1:-}" in
  test-up)   T up -d --wait mysql redis mongo kafka pgvector && T build -q rag ;;
  test-java) shift
    docker run --rm --user "$ME" --network agentflow-coursetest_default -v "$PWD":/src -v "$HOME/.m2":/m2 -w /src \
      -e SPRING_DATASOURCE_URL='jdbc:mysql://mysql:3306/agentflow?useSSL=false&allowPublicKeyRetrieval=true&serverTimezone=Asia/Shanghai' \
      -e SPRING_KAFKA_BOOTSTRAP_SERVERS=kafka:19092 -e SPRING_DATA_REDIS_HOST=redis \
      -e SPRING_DATA_MONGODB_URI=mongodb://mongo:27017/agentflow_trace \
      maven:3.9-eclipse-temurin-21 mvn -B -q -Duser.home=/m2 -Dmaven.repo.local=/m2/repository "$@" ;;
  test-rag)  shift
    docker run --rm --user "$ME" --network agentflow-coursetest_default -v "$PWD/agentflow-rag":/app -w /app \
      -e HOME=/tmp -e PYTHONDONTWRITEBYTECODE=1 \
      -e RAG_DB_URL=postgresql://postgres:agentflow@pgvector:5432/agentflow_rag_test \
      agentflow-coursetest-rag python -m pytest -q -p no:cacheprovider "$@" ;;
  test-down) T down -v ;;
  up)        C up -d --build --wait ;;
  down)      C down ;;
  *) echo "usage: $0 test-up | test-java <mvn args> | test-rag [pytest args] | test-down | up | down" >&2; exit 2 ;;
esac
