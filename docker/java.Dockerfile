# 通用 Java 镜像:构建参数 MODULE=agentflow-server | agentflow-worker
FROM maven:3.9-eclipse-temurin-17 AS build
ARG MODULE
WORKDIR /src
COPY pom.xml .
COPY agentflow-common/pom.xml agentflow-common/
COPY agentflow-server/pom.xml agentflow-server/
COPY agentflow-worker/pom.xml agentflow-worker/
# 先只拷 pom 拉依赖:源码变动时这一层走缓存
RUN mvn -B -q -pl ${MODULE} -am dependency:go-offline || true
COPY agentflow-common agentflow-common
COPY agentflow-server agentflow-server
COPY agentflow-worker agentflow-worker
RUN mvn -B -q -pl ${MODULE} -am package -DskipTests

FROM eclipse-temurin:17-jre
ARG MODULE
WORKDIR /app
COPY --from=build /src/${MODULE}/target/${MODULE}-*.jar app.jar
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
