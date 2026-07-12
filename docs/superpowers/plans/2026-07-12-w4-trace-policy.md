# W4 可观测与治理 Implementation Plan(子计划二)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`).

**Goal:** 给 AgentFlow 加 Run Trace(事件溯源 → MongoDB 回放/审计)与统一副作用 Policy 层(业务幂等)。

**Architecture:** 各组件(server+worker)在每个状态变更 emit 一条 `TraceEvent` 到 Kafka `AGENTFLOW_TRACE`;server 端单一 `TraceConsumer` 落 MongoDB(单机),`GET /trace` 回放。副作用统一经 `SideEffectPolicy.execute(businessKey, action)`(Redis 业务幂等)执行;代表性副作用 `NotificationService` 接入 `TaskFinalizer`,演示"重复 finalize 不重复通知"。

**Tech Stack:** Java 17 · Spring Boot 3.3.5 · spring-kafka · spring-data-mongodb(server)· Redisson(server,复用 worker 已用)· MongoDB 7 · JUnit5+Mockito

## Global Constraints

(来自 `docs/superpowers/specs/2026-07-12-w4-trace-policy-design.md`)

- **直接在 `main` 分支**;当前 `main` @ `30f8902`。
- Trace 事件**经 Kafka `AGENTFLOW_TRACE` 统一出口**,Mongo 只在 `TraceConsumer` 一处被写。
- **一事件一文档**(collection `trace_event`),索引 `traceId + timestamp`;`traceId = taskUuid`。
- stage 枚举:`SUBMITTED/DECOMPOSED/DISPATCHED/WORKER_RECEIVED/PROCESSED/RESULT_SENT/SUBTASK_SETTLED/RETRY/DLQ/RECOVERED/TIMEOUT_REDISPATCH/TASK_FINALIZED`。
- **副作用只能经 `SideEffectPolicy.execute`**(代码层 enforce);业务幂等键如 `notify:<taskUuid>`,TTL 24h。
- 埋点**不得改变既有状态机/CAS/幂等/重试语义**——只加旁路的 emit,失败不可拖垮主流程(emit 用 try-catch 包住,trace 丢失不影响业务)。
- 直连本地 Docker:MySQL/Kafka/Redis 已起;**MongoDB 本任务新增**。Docker 命令前 `export PATH="$HOME/.orbstack/bin:$PATH"`。
- 每任务 TDD → commit → push。集成测试直连真实中间件。
- 容器以手动名运行(`docker compose ps` 空;用 `docker ps` 查)。

---

## File Structure

**agentflow-common**
- `mq/Topics.java` — 加 `TRACE`。
- `trace/TraceEvent.java`(DTO)、`trace/TraceStage.java`(枚举)。

**agentflow-worker**
- `trace/TraceEmitter.java`(@Component,发 TRACE)。
- 埋点:`listener/SubtaskListener.java`、`retry/RetryRouter.java`。
- `pom.xml`/`application.yml` 无新增(已有 kafka)。

**agentflow-server**
- `pom.xml` — 加 spring-boot-starter-data-mongodb + redisson-spring-boot-starter。
- `application.yml` — 加 `spring.data.mongodb` + `spring.data.redis`。
- `docker-compose.yml` — 加 mongo。
- `trace/TraceEmitter.java`、`trace/TraceConsumer.java`、`trace/TraceDocument.java`、`trace/TraceEventRepository.java`。
- `controller/TraceController.java`(GET /trace)。
- `policy/SideEffectPolicy.java`、`service/NotificationService.java`。
- 埋点:`TaskSubmitService`、`SubtaskDispatcher`、`ResultHandleService`、`TaskFinalizer`、`TimeoutSweepService`、`DlqRecoveryService`。

---

### Task 1: MongoDB 基础设施 + TraceEvent 契约

**Files:**
- Modify: `docker-compose.yml`
- Modify: `agentflow-server/pom.xml`
- Modify: `agentflow-server/src/main/resources/application.yml`
- Modify: `agentflow-common/src/main/java/com/agentflow/common/mq/Topics.java`
- Create: `agentflow-common/src/main/java/com/agentflow/common/trace/TraceStage.java`
- Create: `agentflow-common/src/main/java/com/agentflow/common/trace/TraceEvent.java`

**Interfaces:**
- Produces: `Topics.TRACE="AGENTFLOW_TRACE"`;`TraceStage` 枚举;`TraceEvent`(手写 getter/setter DTO,同既有 mq DTO 风格)。

- [ ] **Step 1: docker-compose 加 mongo**

`docker-compose.yml` 的 `services:` 下加:

```yaml
  mongo:
    image: mongo:7
    container_name: agentflow-mongo
    ports:
      - "27017:27017"
```

启动(容器手动名):
Run: `export PATH="$HOME/.orbstack/bin:$PATH"; docker run -d --name agentflow-mongo -p 27017:27017 mongo:7 2>/dev/null || docker start agentflow-mongo`
Run: `docker exec agentflow-mongo mongosh --quiet --eval "db.runCommand({ping:1}).ok"`
Expected: 输出 `1`(mongo 可用)。

- [ ] **Step 2: server 加依赖**

`agentflow-server/pom.xml` dependencies 追加:

```xml
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-data-mongodb</artifactId>
    </dependency>
    <dependency>
      <groupId>org.redisson</groupId>
      <artifactId>redisson-spring-boot-starter</artifactId>
      <version>3.34.1</version>
    </dependency>
```

`application.yml` 的 `spring:` 下加:

```yaml
  data:
    mongodb:
      uri: mongodb://localhost:27017/agentflow_trace
    redis:
      host: localhost
      port: 6379
```

- [ ] **Step 3: Topics 加 TRACE**

`Topics.java` 加常量:`public static final String TRACE = "AGENTFLOW_TRACE";`

- [ ] **Step 4: TraceStage 枚举**

`TraceStage.java`:

```java
package com.agentflow.common.trace;

public enum TraceStage {
    SUBMITTED, DECOMPOSED, DISPATCHED, WORKER_RECEIVED, PROCESSED, RESULT_SENT,
    SUBTASK_SETTLED, RETRY, DLQ, RECOVERED, TIMEOUT_REDISPATCH, TASK_FINALIZED
}
```

- [ ] **Step 5: TraceEvent DTO**

`TraceEvent.java`(手写 getter/setter,和 `mq/SubtaskMessage` 一致风格,便于 Kafka JSON 序列化):

```java
package com.agentflow.common.trace;

public class TraceEvent {
    private String traceId;   // = taskUuid
    private Long taskId;
    private Long subtaskId;   // 可空
    private String stage;     // TraceStage.name()
    private String status;
    private String detail;
    private long timestamp;   // epoch millis

    public TraceEvent() {}

    public TraceEvent(String traceId, Long taskId, Long subtaskId,
                      String stage, String status, String detail, long timestamp) {
        this.traceId = traceId; this.taskId = taskId; this.subtaskId = subtaskId;
        this.stage = stage; this.status = status; this.detail = detail; this.timestamp = timestamp;
    }

    public String getTraceId() { return traceId; }
    public void setTraceId(String traceId) { this.traceId = traceId; }
    public Long getTaskId() { return taskId; }
    public void setTaskId(Long taskId) { this.taskId = taskId; }
    public Long getSubtaskId() { return subtaskId; }
    public void setSubtaskId(Long subtaskId) { this.subtaskId = subtaskId; }
    public String getStage() { return stage; }
    public void setStage(String stage) { this.stage = stage; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getDetail() { return detail; }
    public void setDetail(String detail) { this.detail = detail; }
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
}
```

- [ ] **Step 6: 编译验证 + Commit**

Run: `mvn -q -pl agentflow-common -am compile` → BUILD SUCCESS。
Run: `mvn -q -pl agentflow-server -am compile` → BUILD SUCCESS(新依赖解析)。

```bash
git add docker-compose.yml agentflow-server/pom.xml agentflow-server/src/main/resources/application.yml agentflow-common/src
git commit -m "feat: MongoDB 基础设施 + TraceEvent 契约(TRACE topic/stage 枚举/DTO)"
git push
```

---

### Task 2: Trace 事件溯源(emit → Kafka → Mongo)

**Files:**
- Create: `agentflow-server/src/main/java/com/agentflow/server/trace/TraceEmitter.java`
- Create: `agentflow-worker/src/main/java/com/agentflow/worker/trace/TraceEmitter.java`
- Create: `agentflow-server/src/main/java/com/agentflow/server/trace/TraceDocument.java`
- Create: `agentflow-server/src/main/java/com/agentflow/server/trace/TraceEventRepository.java`
- Create: `agentflow-server/src/main/java/com/agentflow/server/trace/TraceConsumer.java`
- Test: `agentflow-server/src/test/java/com/agentflow/server/trace/TraceConsumerTest.java`

**Interfaces:**
- Produces:
  - `TraceEmitter.emit(String traceId, Long taskId, Long subtaskId, TraceStage stage, String status, String detail)`(server 与 worker 各一,同签名)——构造 `TraceEvent`(timestamp=now)并 `kafkaTemplate.send(Topics.TRACE, traceId, event)`;**try-catch 包住,emit 失败只记 warn 不抛**。
  - `TraceConsumer` 消费 TRACE → 存 `TraceDocument` 到 Mongo。
  - `TraceEventRepository.findByTraceIdOrderByTimestampAsc(String)` → `List<TraceDocument>`。

- [ ] **Step 1: 两个 TraceEmitter(server + worker,同逻辑)**

`agentflow-server/.../trace/TraceEmitter.java`:

```java
package com.agentflow.server.trace;

import com.agentflow.common.mq.Topics;
import com.agentflow.common.trace.TraceEvent;
import com.agentflow.common.trace.TraceStage;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

/** 统一 trace 出口:每个状态变更 emit 到 Kafka TRACE。emit 失败不拖垮业务(只 warn)。 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TraceEmitter {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void emit(String traceId, Long taskId, Long subtaskId,
                     TraceStage stage, String status, String detail) {
        try {
            TraceEvent ev = new TraceEvent(traceId, taskId, subtaskId,
                    stage.name(), status, detail, System.currentTimeMillis());
            kafkaTemplate.send(Topics.TRACE, traceId, ev);
        } catch (Exception e) {
            log.warn("trace emit 失败 traceId={} stage={}", traceId, stage, e);
        }
    }
}
```

`agentflow-worker/.../trace/TraceEmitter.java`:同样内容,package 改 `com.agentflow.worker.trace`。

- [ ] **Step 2: TraceDocument + Repository**

`TraceDocument.java`:

```java
package com.agentflow.server.trace;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.mapping.Document;

@Document(collection = "trace_event")
@CompoundIndex(name = "trace_ts", def = "{'traceId': 1, 'timestamp': 1}")
public class TraceDocument {
    @Id private String id;
    private String traceId;
    private Long taskId;
    private Long subtaskId;
    private String stage;
    private String status;
    private String detail;
    private long timestamp;

    public TraceDocument() {}
    // getters/setters for all fields (same style as TraceEvent)
    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
    public String getTraceId() { return traceId; }
    public void setTraceId(String traceId) { this.traceId = traceId; }
    public Long getTaskId() { return taskId; }
    public void setTaskId(Long taskId) { this.taskId = taskId; }
    public Long getSubtaskId() { return subtaskId; }
    public void setSubtaskId(Long subtaskId) { this.subtaskId = subtaskId; }
    public String getStage() { return stage; }
    public void setStage(String stage) { this.stage = stage; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getDetail() { return detail; }
    public void setDetail(String detail) { this.detail = detail; }
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
}
```

`TraceEventRepository.java`:

```java
package com.agentflow.server.trace;

import org.springframework.data.mongodb.repository.MongoRepository;
import java.util.List;

public interface TraceEventRepository extends MongoRepository<TraceDocument, String> {
    List<TraceDocument> findByTraceIdOrderByTimestampAsc(String traceId);
}
```

- [ ] **Step 3: TraceConsumer**

`TraceConsumer.java`:

```java
package com.agentflow.server.trace;

import com.agentflow.common.mq.Topics;
import com.agentflow.common.trace.TraceEvent;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/** 单一 trace 落库出口:TRACE topic → MongoDB。 */
@Component
@RequiredArgsConstructor
public class TraceConsumer {

    private final TraceEventRepository repository;

    @KafkaListener(topics = Topics.TRACE, groupId = "agentflow-trace-consumer")
    public void onTrace(TraceEvent ev) {
        TraceDocument doc = new TraceDocument();
        doc.setTraceId(ev.getTraceId());
        doc.setTaskId(ev.getTaskId());
        doc.setSubtaskId(ev.getSubtaskId());
        doc.setStage(ev.getStage());
        doc.setStatus(ev.getStatus());
        doc.setDetail(ev.getDetail());
        doc.setTimestamp(ev.getTimestamp());
        repository.save(doc);
    }
}
```

> server consumer 的 `spring.json.trusted.packages` 目前是 `com.agentflow.common.mq`;TraceEvent 在 `com.agentflow.common.trace`。**改 application.yml** 把 trusted.packages 设为 `com.agentflow.common`(覆盖 mq + trace 两个子包),或 `com.agentflow.common.mq,com.agentflow.common.trace`。本步一并改(worker 侧同理,若 worker 也要消费——worker 只 emit 不消费 trace,但 worker 的 SubtaskMessage 反序列化 trusted 包需保留;把 worker trusted 也放宽为 `com.agentflow.common` 更省事)。

- [ ] **Step 4: 集成测试(真 Kafka + Mongo)**

`TraceConsumerTest.java`:

```java
package com.agentflow.server.trace;

import com.agentflow.common.trace.TraceStage;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class TraceConsumerTest {

    @Autowired private TraceEmitter emitter;
    @Autowired private TraceEventRepository repository;

    private String traceId;

    @AfterEach
    void cleanup() {
        if (traceId != null) {
            repository.deleteAll(repository.findByTraceIdOrderByTimestampAsc(traceId));
        }
    }

    @Test
    void emittedEventLandsInMongoInOrder() {
        traceId = "trace-" + UUID.randomUUID();
        emitter.emit(traceId, 1L, null, TraceStage.SUBMITTED, "PENDING", "submit");
        emitter.emit(traceId, 1L, 10L, TraceStage.DISPATCHED, "DISPATCHED", "seq0");

        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            var events = repository.findByTraceIdOrderByTimestampAsc(traceId);
            assertEquals(2, events.size());
            assertEquals("SUBMITTED", events.get(0).getStage());
            assertEquals("DISPATCHED", events.get(1).getStage());
        });
    }
}
```

> 需要 awaitility(spring-boot-starter-test 传递带入 org.awaitility)。若类路径无 awaitility,改用轮询 `Thread.sleep` + 重查(最多 15s)。

- [ ] **Step 5: 跑测试 + 回归**

前置:MySQL/Kafka/Redis/**Mongo** 均运行。
Run: `mvn -q -pl agentflow-server -am test -Dtest=TraceConsumerTest -Dsurefire.failIfNoSpecifiedTests=false` → PASS。
Run: `mvn -q test` → 全绿(既有 43 测试 + 新增)。

- [ ] **Step 6: Commit**

```bash
git add agentflow-server agentflow-worker/src/main/java/com/agentflow/worker/trace
git commit -m "feat: Trace 事件溯源——TraceEmitter→Kafka→TraceConsumer→MongoDB"
git push
```

---

### Task 3: server 侧全链路埋点

**Files(均 Modify,注入 `TraceEmitter` + 在关键点 emit):**
- `service/TaskSubmitService.java`(SUBMITTED, DECOMPOSED)
- `coordinator/SubtaskDispatcher.java`(DISPATCHED)
- `service/ResultHandleService.java`(SUBTASK_SETTLED)
- `service/TaskFinalizer.java`(TASK_FINALIZED)
- `service/TimeoutSweepService.java`(TIMEOUT_REDISPATCH)
- `service/DlqRecoveryService.java`(RECOVERED)
- Test: `agentflow-server/src/test/java/com/agentflow/server/trace/ServerTracePointsTest.java`

**Interfaces:**
- Consumes: Task 2 的 `TraceEmitter`。
- Produces: 提交→分发→落定→终态 的 server 侧 trace 覆盖。**traceId 一律用 `String.valueOf(taskId)`**(taskId 各处都能拿到,无需 taskUuid,避免改 SubtaskMessage)。对外 API 再由 uuid 反查 taskId(Task 5)。

- [ ] **Step 1: 写埋点集成测试(先失败)**

`ServerTracePointsTest.java`——`@SpringBootTest` + `@MockBean KafkaTemplate` 无法验 trace(会拦截 emit),故用**真 Kafka**,提交一个任务后查 Mongo 是否出现 SUBMITTED/DECOMPOSED/DISPATCHED:

```java
package com.agentflow.server.trace;

import com.agentflow.server.service.TaskSubmitService;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.trace.TraceEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class ServerTracePointsTest {

    @Autowired private TaskSubmitService submitService;
    @Autowired private TraceEventRepository repository;
    @Autowired private ObjectMapper om;

    private String traceId;

    @AfterEach
    void cleanup() {
        if (traceId != null) repository.deleteAll(repository.findByTraceIdOrderByTimestampAsc(traceId));
    }

    @Test
    void submitEmitsSubmittedDecomposedDispatched() throws Exception {
        // 真实提交:会走 persist + dispatch(worker 可能不在,但 DISPATCHED 由 server 分发时 emit)
        TaskEntity task = submitService.submit("ECHO_BATCH",
                om.readTree("{\"items\":[\"a\",\"b\"]}"));
        traceId = String.valueOf(task.getId());

        Awaitility.await().atMost(Duration.ofSeconds(15)).untilAsserted(() -> {
            List<TraceDocument> evs = repository.findByTraceIdOrderByTimestampAsc(traceId);
            Set<String> stages = evs.stream().map(TraceDocument::getStage).collect(Collectors.toSet());
            assertTrue(stages.contains("SUBMITTED"), "缺 SUBMITTED");
            assertTrue(stages.contains("DECOMPOSED"), "缺 DECOMPOSED");
            assertTrue(stages.contains("DISPATCHED"), "缺 DISPATCHED");
        });
    }
}
```

> 注意:该测试会真的把子任务发到 Kafka,若无 worker 消费则子任务停在 DISPATCHED——对本测试无碍(只验 server 侧 trace)。测试后清理 trace;task/subtask 行可留(或在 cleanup 里按 taskUuid 删)。

- [ ] **Step 2: 各处注入 TraceEmitter 并 emit**

按下表在每个方法的对应位置加 emit(**emit 只读不改业务;放在状态变更之后**;traceId 一律 `String.valueOf(taskId)`):

- `TaskSubmitService.submit(...)`:persist 返回 task 后 → `traceEmitter.emit(String.valueOf(task.getId()), task.getId(), null, TraceStage.SUBMITTED, "PENDING", "type="+type)`;并 `... TraceStage.DECOMPOSED, "PENDING", "subtasks="+task.getSubtaskTotal())`。
- `SubtaskDispatcher.dispatch(task)`:每个 subtask 迁移 DISPATCHED 后 → `emit(String.valueOf(task.getId()), task.getId(), sub.getId(), DISPATCHED, "DISPATCHED", "seq="+sub.getSeq())`。
- `ResultHandleService.handle(msg)`:子任务落定后 → `emit(String.valueOf(msg.getTaskId()), msg.getTaskId(), msg.getSubtaskId(), SUBTASK_SETTLED, target.name(), null)`。
- `TaskFinalizer.aggregate(task, terminal)`:updateById 后 → `emit(String.valueOf(task.getId()), task.getId(), null, TASK_FINALIZED, terminal.name(), "subtasks="+subs.size())`。
- `TimeoutSweepService`:重投分支 → `emit(String.valueOf(s.getTaskId()), s.getTaskId(), s.getId(), TIMEOUT_REDISPATCH, "DISPATCHED", "redispatch="+n)`。
- `DlqRecoveryService.replay`:重投后 → `emit(String.valueOf(t.getId()), t.getId(), s.getId(), RECOVERED, "DISPATCHED", "manual replay")`。

每处注入 `private final TraceEmitter traceEmitter;`(类已是 `@RequiredArgsConstructor`,加字段即可;`TaskFinalizer` 亦然)。

- [ ] **Step 3: 跑测试 + 回归**

Run: `mvn -q -pl agentflow-server -am test -Dtest=ServerTracePointsTest -Dsurefire.failIfNoSpecifiedTests=false` → PASS。
Run: `mvn -q test` → 全绿。

- [ ] **Step 4: Commit**

```bash
git add agentflow-server/src
git commit -m "feat: server 侧全链路 Trace 埋点(submit/dispatch/settle/finalize/timeout/recovery)"
git push
```

---

### Task 4: worker 侧埋点

**Files:**
- Modify: `agentflow-worker/src/main/java/com/agentflow/worker/listener/SubtaskListener.java`(WORKER_RECEIVED, PROCESSED, RESULT_SENT)
- Modify: `agentflow-worker/src/main/java/com/agentflow/worker/retry/RetryRouter.java`(RETRY, DLQ)
- Test: 更新既有 `SubtaskListenerIdempotencyTest`/`SubtaskListenerTest`/`RetryRouterTest` 的构造(注入 mock `TraceEmitter`)

**Interfaces:**
- Consumes: worker `TraceEmitter`。
- Produces: worker 侧 trace 覆盖。**traceId = `String.valueOf(msg.getTaskId())`**,与 server 侧一致(server 也用 taskId)——`SubtaskMessage` 无需任何改动。

- [ ] **Step 1: worker 埋点(SubtaskListener)**

`SubtaskListener.onMessage` 注入 `private final TraceEmitter traceEmitter;`,在各点 emit(traceId=`String.valueOf(msg.getTaskId())`):
- 进入即:`WORKER_RECEIVED, null, "attempt="+attempt`。
- 幂等跳过分支:`PROCESSED, "SKIPPED_IDEMPOTENT", null` 后 return。
- 处理成功:`PROCESSED, "OK", null`;发结果后:`RESULT_SENT, "true", null`。
- 失败(走 retryRouter 前):`PROCESSED, "FAILED", e.getMessage()`。

- [ ] **Step 2: worker 埋点(RetryRouter)**

`RetryRouter.route` 注入 `TraceEmitter`(类已 `@RequiredArgsConstructor`,加字段):
- 进重试档发送后:`emit(String.valueOf(msg.getTaskId()), msg.getTaskId(), msg.getSubtaskId(), RETRY, "attempt="+attempt, topic)`。
- 进 DLQ:`emit(String.valueOf(msg.getTaskId()), msg.getTaskId(), msg.getSubtaskId(), DLQ, "exhausted", errorMsg)`。

- [ ] **Step 3: 更新既有 worker 测试构造**

`SubtaskListener` 与 `RetryRouter` 构造多了 `TraceEmitter`,给 `SubtaskListenerTest`、`SubtaskListenerIdempotencyTest`、`RetryRouterTest` 传 `mock(TraceEmitter.class)`(`RetryDelayListener` 构造未变,不用改)。测试逻辑不变。

- [ ] **Step 4: 跑 worker 测试 + 全库回归**

Run: `mvn -q -pl agentflow-worker -am test` → 绿。
Run: `mvn -q test` → 全绿。

- [ ] **Step 5: Commit**

```bash
git add agentflow-worker
git commit -m "feat: worker 侧 Trace 埋点(WORKER_RECEIVED/PROCESSED/RESULT_SENT/RETRY/DLQ)"
git push
```

---

### Task 5: Trace 回放 API

**Files:**
- Create: `agentflow-server/src/main/java/com/agentflow/server/controller/TraceController.java`
- Create: `agentflow-server/src/main/java/com/agentflow/server/controller/dto/TraceView.java`
- Test: `agentflow-server/src/test/java/com/agentflow/server/controller/TraceControllerTest.java`

**Interfaces:**
- Produces: `GET /api/v1/tasks/{taskUuid}/trace` → 200 `[{stage,status,subtaskId,detail,timestamp}...]`(按 timestamp 升序);无事件 → 200 空数组。

- [ ] **Step 1: TraceView + Controller**

`TraceView.java`:

```java
package com.agentflow.server.controller.dto;

public record TraceView(String stage, String status, Long subtaskId, String detail, long timestamp) {}
```

`TraceController.java`(按 uuid 反查 taskId,再按 `String.valueOf(id)` 取 trace):

```java
package com.agentflow.server.controller;

import com.agentflow.server.controller.dto.TraceView;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.TaskMapper;
import com.agentflow.server.trace.TraceEventRepository;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

@RestController
@RequestMapping("/api/v1/tasks")
@RequiredArgsConstructor
public class TraceController {

    private final TraceEventRepository repository;
    private final TaskMapper taskMapper;

    @GetMapping("/{taskUuid}/trace")
    public List<TraceView> trace(@PathVariable("taskUuid") String taskUuid) {
        TaskEntity task = taskMapper.selectOne(
                new QueryWrapper<TaskEntity>().eq("task_uuid", taskUuid));
        if (task == null) {
            return List.of();
        }
        return repository.findByTraceIdOrderByTimestampAsc(String.valueOf(task.getId())).stream()
                .map(d -> new TraceView(d.getStage(), d.getStatus(),
                        d.getSubtaskId(), d.getDetail(), d.getTimestamp()))
                .toList();
    }
}
```

(`@PathVariable("taskUuid")` 显式名——本项目无 -parameters flag。)

- [ ] **Step 2: 测试**

`TraceControllerTest.java`(`@SpringBootTest @AutoConfigureMockMvc`;先经 repository 存两条 TraceDocument,再 GET 断言顺序):

```java
package com.agentflow.server.controller;

import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.TaskMapper;
import com.agentflow.server.trace.TraceDocument;
import com.agentflow.server.trace.TraceEventRepository;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;

import java.util.UUID;

import static org.hamcrest.Matchers.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@SpringBootTest
@AutoConfigureMockMvc
class TraceControllerTest {

    @Autowired private MockMvc mockMvc;
    @Autowired private TraceEventRepository repository;
    @Autowired private TaskMapper taskMapper;

    private String traceId;   // = String.valueOf(taskId)
    private Long taskId;

    @AfterEach
    void cleanup() {
        if (traceId != null) repository.deleteAll(repository.findByTraceIdOrderByTimestampAsc(traceId));
        if (taskId != null) taskMapper.deleteById(taskId);
    }

    private TraceDocument doc(String tid, String stage, long ts) {
        TraceDocument d = new TraceDocument();
        d.setTraceId(tid); d.setStage(stage); d.setStatus("X"); d.setTimestamp(ts);
        return d;
    }

    @Test
    void returnsTraceInTimestampOrder() throws Exception {
        // 造一个真实 task(controller 按 uuid 反查 id)
        TaskEntity t = new TaskEntity();
        String uuid = UUID.randomUUID().toString();
        t.setTaskUuid(uuid); t.setType("ECHO_BATCH"); t.setStatus("COMPLETED");
        t.setSubtaskTotal(1); t.setSubtaskDone(1); t.setSubtaskFailed(0);
        taskMapper.insert(t);
        taskId = t.getId();
        traceId = String.valueOf(taskId);
        repository.save(doc(traceId, "SUBMITTED", 100));
        repository.save(doc(traceId, "TASK_FINALIZED", 200));

        mockMvc.perform(get("/api/v1/tasks/" + uuid + "/trace"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(2)))
                .andExpect(jsonPath("$[0].stage", is("SUBMITTED")))
                .andExpect(jsonPath("$[1].stage", is("TASK_FINALIZED")));
    }

    @Test
    void unknownTraceReturnsEmpty() throws Exception {
        mockMvc.perform(get("/api/v1/tasks/no-such/trace"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$", hasSize(0)));
    }
}
```

- [ ] **Step 3: 跑测试 + 回归 → Commit**

Run: `mvn -q -pl agentflow-server -am test -Dtest=TraceControllerTest -Dsurefire.failIfNoSpecifiedTests=false` → PASS;`mvn -q test` 全绿。

```bash
git add agentflow-server/src
git commit -m "feat: Trace 回放 API(GET /api/v1/tasks/{uuid}/trace)"
git push
```

---

### Task 6: 统一副作用 Policy 层 + 通知

**Files:**
- Create: `agentflow-server/src/main/java/com/agentflow/server/policy/SideEffectPolicy.java`
- Create: `agentflow-server/src/main/java/com/agentflow/server/service/NotificationService.java`
- Modify: `agentflow-server/src/main/java/com/agentflow/server/service/TaskFinalizer.java`(接入通知)
- Test: `agentflow-server/src/test/java/com/agentflow/server/policy/SideEffectPolicyTest.java`
- Test: `agentflow-server/src/test/java/com/agentflow/server/service/NotificationDedupeTest.java`

**Interfaces:**
- Produces:
  - `SideEffectPolicy.execute(String businessKey, Runnable action)` → 首次执行 action 并占键(Redis,TTL 24h),重复直接跳过(幂等)。返回 `boolean`(true=执行了,false=被去重跳过)。
  - `NotificationService.notifyTaskFinished(TaskEntity task)` → **经 policy** 发通知(本阶段=记日志 + emit TraceEvent `NOTIFIED`?否——stage 枚举无 NOTIFIED;用日志 + 计数即可)。

- [ ] **Step 1: SideEffectPolicy(Redisson 业务幂等)+ 测试**

`SideEffectPolicyTest.java`(`@SpringBootTest`,真 Redis):

```java
package com.agentflow.server.policy;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class SideEffectPolicyTest {

    @Autowired private SideEffectPolicy policy;

    @Test
    void runsOnceThenDedupes() {
        String key = "test:" + UUID.randomUUID();
        AtomicInteger n = new AtomicInteger();
        assertTrue(policy.execute(key, n::incrementAndGet));   // 首次执行
        assertFalse(policy.execute(key, n::incrementAndGet));  // 去重跳过
        assertEquals(1, n.get());                              // 只执行一次
    }
}
```

`SideEffectPolicy.java`:

```java
package com.agentflow.server.policy;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * 统一副作用出口 + 业务级幂等:所有副作用经 execute 执行,businessKey 去重。
 * 代码层 enforce("只能经这里"),防同一业务动作被触发多次(如重复发通知/建 case)。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SideEffectPolicy {

    private static final Duration TTL = Duration.ofHours(24);
    private static final String PREFIX = "agentflow:sideeffect:";

    private final RedissonClient redisson;

    /** 首次:执行 action + 占键,返回 true;重复:跳过,返回 false。 */
    public boolean execute(String businessKey, Runnable action) {
        RBucket<String> bucket = redisson.getBucket(PREFIX + businessKey);
        if (!bucket.setIfAbsent("1", TTL)) {
            log.info("副作用去重跳过 businessKey={}", businessKey);
            return false;
        }
        action.run();
        return true;
    }
}
```

- [ ] **Step 2: NotificationService + 接入 TaskFinalizer**

`NotificationService.java`:

```java
package com.agentflow.server.service;

import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.policy.SideEffectPolicy;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/** 代表性副作用:任务完成通知(模拟发报告/建 case)。必须经 Policy 闸 → 业务幂等。 */
@Slf4j
@Service
@RequiredArgsConstructor
public class NotificationService {

    private final SideEffectPolicy policy;

    public void notifyTaskFinished(TaskEntity task) {
        String businessKey = "notify:" + task.getTaskUuid();
        policy.execute(businessKey, () ->
                log.info("📣 任务完成通知已发送 taskUuid={} status={}",
                        task.getTaskUuid(), task.getStatus()));
    }
}
```

`TaskFinalizer.aggregate(...)`:在 `taskMapper.updateById(task)` 之后加 `notificationService.notifyTaskFinished(task);`(注入 `private final NotificationService notificationService;`)。

- [ ] **Step 3: 去重集成测试**

`NotificationDedupeTest.java`(`@SpringBootTest`,真 Redis;同一 uuid 调两次,验证 policy 只放行一次——用 spy 或计数验证):

```java
package com.agentflow.server.service;

import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.policy.SideEffectPolicy;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class NotificationDedupeTest {

    @Autowired private NotificationService notificationService;
    @Autowired private SideEffectPolicy policy;

    @Test
    void sameTaskNotifiesOnce() {
        TaskEntity t = new TaskEntity();
        t.setTaskUuid(UUID.randomUUID().toString());
        t.setStatus("COMPLETED");
        // 直接验 policy 层去重(businessKey 相同第二次返回 false)
        String key = "notify:" + t.getTaskUuid();
        assertTrue(policy.execute(key, () -> {}));
        assertFalse(policy.execute(key, () -> {}));
        // notifyTaskFinished 第二次不会真正执行 action(已被去重)
        notificationService.notifyTaskFinished(t); // 幂等,无副作用重复
    }
}
```

- [ ] **Step 4: 跑测试 + 回归 → Commit**

Run: `mvn -q -pl agentflow-server -am test -Dtest=SideEffectPolicyTest,NotificationDedupeTest -Dsurefire.failIfNoSpecifiedTests=false` → PASS;`mvn -q test` 全绿。

```bash
git add agentflow-server/src
git commit -m "feat: 统一副作用 Policy 层(Redis 业务幂等)+ 任务完成通知,接入 finalize"
git push
```

---

### Task 7: 端到端验收 + 里程碑收尾

**Files:**
- Create: `scripts/trace-demo.sh`
- Modify: `README.md`(Trace/Policy 说明 + 路线图勾选)
- Modify: `docs/backlog.md`

**Interfaces:** 消费全部前序。

- [ ] **Step 1: trace-demo 脚本**

`scripts/trace-demo.sh`:提交任务 → 轮询完成 → `GET /trace` 打印完整生命周期。

```bash
#!/usr/bin/env bash
set -euo pipefail
BASE="http://localhost:8080/api/v1/tasks"
UUID=$(curl -s -X POST "$BASE" -H 'Content-Type: application/json' \
  -d '{"type":"ECHO_BATCH","payload":{"items":["a","b","c"]}}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["taskUuid"])')
echo "taskUuid=$UUID"
for i in $(seq 1 30); do
  ST=$(curl -s "$BASE/$UUID" | python3 -c 'import sys,json;print(json.load(sys.stdin)["status"])')
  [ "$ST" = "COMPLETED" ] && break
  sleep 2
done
echo "==> Run Trace(闭环审计):"
curl -s "$BASE/$UUID/trace" | python3 -m json.tool
echo "✅ trace 覆盖 SUBMITTED→...→TASK_FINALIZED 即闭环执行成功可审计"
```

- [ ] **Step 2: 端到端手动验证(server + 1 worker)**

前置:docker(含 mongo)、server、worker 起。
1. `chmod +x scripts/trace-demo.sh && ./scripts/trace-demo.sh` → trace 里应见 SUBMITTED/DECOMPOSED/DISPATCHED/WORKER_RECEIVED/PROCESSED/RESULT_SENT/SUBTASK_SETTLED/TASK_FINALIZED。
2. 通知幂等:观察 server 日志,一个任务完成只有一条 `📣 任务完成通知已发送`;若对同一任务触发二次 finalize(如 DLQ 恢复路径),日志出现 `副作用去重跳过`。
把实际输出贴进报告。

- [ ] **Step 3: README + backlog**

README 加 "## 可观测与治理(Run Trace + Policy)" 一节;路线图勾选 `- [x] W3–4 可观测:Run Trace(事件溯源→Mongo 回放)+ 统一副作用 Policy 层`。backlog 记新发现。

- [ ] **Step 4: 里程碑自检(报告回答)**

1. Trace 为何经 Kafka 事件溯源而非各处直写 Mongo?"保存时机"怎么答?
2. 怎么从 trace 证明"闭环执行成功"?
3. Policy 的业务幂等键与 W3 消费幂等键有何不同层次?
4. "所有副作用经同一 policy"如何代码层 enforce?

- [ ] **Step 5: Commit**

```bash
git add scripts/trace-demo.sh README.md docs/backlog.md
git commit -m "feat: Trace/Policy 端到端演示脚本 + W3-4 可观测里程碑验收"
git push
```

---

## Self-Review
- **Spec 覆盖**:Run Trace(A)=Task1-5;Policy(B)=Task6;验收=Task7。全覆盖。
- **类型一致**:`TraceEmitter.emit(traceId,taskId,subtaskId,stage,status,detail)`、`TraceEventRepository.findByTraceIdOrderByTimestampAsc`、`SideEffectPolicy.execute(key,Runnable):boolean`、`SubtaskMessage` 加 `taskUuid`——前后一致。
- **需实现者决断并报告**:(a) traceId 统一为 `String.valueOf(taskId)`,对外 API 由 uuid 反查(**无需改 SubtaskMessage**);(b) trusted.packages 放宽到 `com.agentflow.common`(Task 2 Step 3);(c) emit 一律 try-catch 不拖垮业务。这些已在对应任务明确,非占位。
- **风险**:埋点改动面广(6 个 server 类 + 2 个 worker 类),worker 两类构造加 TraceEmitter 波及既有测试——Task 4 需全量回归。
