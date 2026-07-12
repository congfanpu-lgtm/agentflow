# W3 可靠性核心 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 给 W1–2 骨架加上可靠性:崩溃原子性、消费幂等、Kafka 自研阶梯延迟重试 + 死信 + 恢复、超时兜底、优雅停机。

**Architecture:** 在既有 `agentflow-server`(结果回收/状态机/分发)与 `agentflow-worker`(消费/处理)之上增量改造。worker 侧新增幂等守卫(Redisson)与失败路由(重试 topic 阶梯);新增独立的重试延迟消费者(sleep-until-due 后重投主 topic,attempt+1);超 3 次进 DLQ 并回传失败结果。server 侧给 `ResultHandleService.handle` 加事务原子性,新增 `@Scheduled` 超时扫描与 DLQ 恢复重放。

**Tech Stack:** Java 17 · Spring Boot 3.3.5 · spring-kafka · **Redisson 3.34.1**(worker 幂等)· MyBatis-Plus · MySQL 8 · Kafka 4.0 KRaft · JUnit 5 + Mockito

## Global Constraints

(来自 `docs/superpowers/specs/2026-07-12-w3-reliability-core-design.md`,每个任务默认遵守)

- **直接在 `main` 分支实现**(用户指定,不开 worktree);当前 `main` @ `47ecc81`。
- **延迟档位**:`RETRY_5S=5s / RETRY_30S=30s / RETRY_5M=300s`,**最多 3 次重试**,超限进 DLQ。集中为常量,**测试用 profile/属性覆盖为短档**(如 1s)加速。
- **幂等键** = `MD5(subtaskUuid + ":" + input)`,存 Redis,**TTL 24h**;**仅处理成功后置键**,失败不置(保证合法重试能重跑)。用 Redisson。
- **attempt 计数**放 Kafka 消息头 `x-retry-attempt`(缺省 0);与幂等键正交。
- **不引入 ZooKeeper**(W9);**不做 Run Trace / Policy 层**(子计划二)。
- 每完成一个任务:TDD(Red→Green)→ commit → push(`main`,upstream 已设)。
- 集成测试直连本地 Docker:MySQL `localhost:3306`(库 agentflow,root/agentflow123)、Kafka `localhost:9092`、Redis `localhost:6379`,均已 `docker compose up -d` 运行。
- **状态语义复用 W1–2**:Task/Subtask 状态机、CAS、成败双计数、部分失败语义均已存在,勿重造。
- Docker 命令前先 `export PATH="$HOME/.orbstack/bin:$PATH"`(OrbStack)。

---

## File Structure(新增/改动总览)

**agentflow-common**
- Modify `mq/Topics.java` — 增 `RETRY_5S/RETRY_30S/RETRY_5M/DLQ` 常量 + `RETRY_ATTEMPT_HEADER`/`NOT_BEFORE_HEADER`。

**agentflow-worker**
- Modify `pom.xml` — 加 redisson-spring-boot-starter。
- Modify `application.yml` — 加 redis 连接 + consumer `max-poll-interval-ms`/`max-poll-records`。
- Create `idempotency/IdempotencyGuard.java` — Redisson 幂等键。
- Modify `listener/SubtaskListener.java` — 幂等检查 + 失败路由到重试/DLQ。
- Create `retry/RetryProperties.java` — 延迟档位常量(可被属性覆盖)。
- Create `retry/RetryRouter.java` — 按 attempt 决定下一 topic/延迟,封装发送。
- Create `listener/RetryDelayListener.java` — 三个重试 topic 的延迟消费者(sleep-until-due → 重投主 topic,attempt+1)。

**agentflow-server**
- Modify `service/ResultHandleService.java` — `handle` 加 `@Transactional`。
- Modify `config/KafkaTopicConfig.java` — 增 4 个新 topic 的 `NewTopic` bean。
- Modify `docker/mysql/init.sql` + `entity/SubtaskEntity.java` — subtask 增 `redispatch_count` 列。
- Modify `mapper/SubtaskMapper.java` — 增查卡死子任务 + 原子自增/重置的 SQL。
- Create `service/TimeoutSweepService.java` + `scheduler/TimeoutScheduler.java` — 超时扫描重投/失败。
- Create `service/DlqRecoveryService.java` + `listener/DlqListener.java` + `controller/DlqController.java` — DLQ 记录 + 手动重放。
- Modify `ServerApplication.java` — 加 `@EnableScheduling`。

---

### Task 1: 崩溃恢复原子性(`@Transactional`,消化 backlog 🔴)

**Files:**
- Modify: `agentflow-server/src/main/java/com/agentflow/server/service/ResultHandleService.java`
- Test: `agentflow-server/src/test/java/com/agentflow/server/service/ResultHandleCrashTest.java`

**Interfaces:**
- Produces: `ResultHandleService.handle(ResultMessage)` 现在是原子的——子任务落定 + 计数 + finalize 要么全成、要么全滚。

- [ ] **Step 1: 写崩溃回滚测试(不加 @Transactional 于测试类,让 handle 走自身事务)**

`ResultHandleCrashTest.java`:

```java
package com.agentflow.server.service;

import com.agentflow.common.mq.ResultMessage;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doThrow;

// 注意:本类【不加】@Transactional —— 需要 handle 在自己的事务里提交/回滚,由测试直接观察 DB。
@SpringBootTest
class ResultHandleCrashTest {

    @Autowired private ResultHandleService service;
    @Autowired private TaskMapper taskMapper;
    @Autowired private SubtaskMapper subtaskMapper;
    @SpyBean private TaskMapper taskMapperSpy; // 与上面同一个 bean;用于打桩抛异常
    @MockBean private KafkaTemplate<String, Object> kafkaTemplate;

    private Long taskId;
    private Long subId;

    private void seed() {
        TaskEntity t = new TaskEntity();
        t.setTaskUuid(UUID.randomUUID().toString());
        t.setType("ECHO_BATCH");
        t.setStatus("RUNNING");
        t.setSubtaskTotal(1);
        t.setSubtaskDone(0);
        t.setSubtaskFailed(0);
        taskMapper.insert(t);
        taskId = t.getId();
        SubtaskEntity s = new SubtaskEntity();
        s.setSubtaskUuid(UUID.randomUUID().toString());
        s.setTaskId(taskId);
        s.setSeq(0);
        s.setStatus("DISPATCHED");
        s.setInput("{\"text\":\"x\"}");
        subtaskMapper.insert(s);
        subId = s.getId();
    }

    @AfterEach
    void cleanup() {
        if (subId != null) subtaskMapper.deleteById(subId);
        if (taskId != null) taskMapper.deleteById(taskId);
    }

    @Test
    void crashBetweenSettleAndIncrementRollsBackSubtask() {
        seed();
        // 在计数自增处注入崩溃(RuntimeException 默认触发回滚)
        doThrow(new RuntimeException("模拟崩溃")).when(taskMapperSpy).incrementDone(taskId);

        assertThrows(RuntimeException.class, () ->
                service.handle(new ResultMessage(taskId, subId, true,
                        "{\"echo\":\"X\",\"length\":1}", null)));

        // 关键断言:子任务状态回滚到 DISPATCHED(而非停留在 COMPLETED),计数未变
        assertEquals("DISPATCHED", subtaskMapper.selectById(subId).getStatus());
        TaskEntity t = taskMapper.selectById(taskId);
        assertEquals(0, t.getSubtaskDone());
        assertEquals("RUNNING", t.getStatus());
    }
}
```

- [ ] **Step 2: 跑测试确认失败**

前置:`docker compose up -d` 已运行。
Run: `mvn -q -pl agentflow-server -am test -Dtest=ResultHandleCrashTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: FAIL —— 无 `@Transactional` 时子任务已提交为 COMPLETED,断言 `DISPATCHED` 失败。

- [ ] **Step 3: 加 @Transactional**

`ResultHandleService.java`:import 增 `import org.springframework.transaction.annotation.Transactional;`;给 `handle` 方法加注解:

```java
    @Transactional
    public void handle(ResultMessage msg) {
```

(其余方法体不变。`handle` 由 `ResultListener` 外部调用,走 Spring 代理,注解生效。)

- [ ] **Step 4: 跑测试确认通过 + 全库回归**

Run: `mvn -q -pl agentflow-server -am test -Dtest=ResultHandleCrashTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: PASS。
Run: `mvn -q test`
Expected: 全部 PASS(W1–2 的 ResultHandleServiceTest 等不受影响)。

- [ ] **Step 5: Commit**

```bash
git add agentflow-server/src
git commit -m "fix: ResultHandleService.handle 加 @Transactional,消化 backlog 崩溃恢复缺口"
git push
```

---

### Task 2: 消费幂等(Redisson + MD5)

**Files:**
- Modify: `agentflow-worker/pom.xml`
- Modify: `agentflow-worker/src/main/resources/application.yml`
- Create: `agentflow-worker/src/main/java/com/agentflow/worker/idempotency/IdempotencyGuard.java`
- Modify: `agentflow-worker/src/main/java/com/agentflow/worker/listener/SubtaskListener.java`
- Test: `agentflow-worker/src/test/java/com/agentflow/worker/idempotency/IdempotencyGuardTest.java`
- Test: `agentflow-worker/src/test/java/com/agentflow/worker/listener/SubtaskListenerIdempotencyTest.java`

**Interfaces:**
- Produces:
  - `IdempotencyGuard.key(String subtaskUuid, String input)` → `String`(MD5 hex)。
  - `IdempotencyGuard.alreadyProcessed(String key)` → `boolean`。
  - `IdempotencyGuard.markProcessed(String key)` → void(置键,TTL 24h)。
  - `SubtaskListener` 在处理前查 `alreadyProcessed`,成功后 `markProcessed`。

- [ ] **Step 1: 加 Redisson 依赖与 redis 配置**

`agentflow-worker/pom.xml` dependencies 追加:

```xml
    <dependency>
      <groupId>org.redisson</groupId>
      <artifactId>redisson-spring-boot-starter</artifactId>
      <version>3.34.1</version>
    </dependency>
```

`agentflow-worker/src/main/resources/application.yml` 的 `spring:` 下追加:

```yaml
  data:
    redis:
      host: localhost
      port: 6379
```

> redisson-spring-boot-starter 会依据 `spring.data.redis.*` 自动装配一个 `RedissonClient` bean。

- [ ] **Step 2: 写 IdempotencyGuard 集成测试(连真实 Redis)**

`IdempotencyGuardTest.java`:

```java
package com.agentflow.worker.idempotency;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class IdempotencyGuardTest {

    @Autowired private IdempotencyGuard guard;

    @Test
    void sameContentSameKey() {
        String k1 = guard.key("uuid-1", "{\"text\":\"a\"}");
        String k2 = guard.key("uuid-1", "{\"text\":\"a\"}");
        assertEquals(k1, k2);
        assertNotEquals(k1, guard.key("uuid-2", "{\"text\":\"a\"}"));
    }

    @Test
    void markThenAlreadyProcessed() {
        String key = guard.key("uuid-" + System.nanoTime(), "{\"text\":\"a\"}");
        assertFalse(guard.alreadyProcessed(key));
        guard.markProcessed(key);
        assertTrue(guard.alreadyProcessed(key));
    }
}
```

- [ ] **Step 3: 实现 IdempotencyGuard**

`IdempotencyGuard.java`:

```java
package com.agentflow.worker.idempotency;

import lombok.RequiredArgsConstructor;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.HexFormat;

/**
 * 消费幂等:幂等键 = MD5(subtaskUuid + ":" + input),仅处理成功后置键(TTL 24h)。
 * 挡住"重复投递已成功消息"的重复干活;失败不置键,合法重试可重跑。
 */
@Component
@RequiredArgsConstructor
public class IdempotencyGuard {

    private static final Duration TTL = Duration.ofHours(24);
    private static final String PREFIX = "agentflow:idem:";

    private final RedissonClient redisson;

    public String key(String subtaskUuid, String input) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest((subtaskUuid + ":" + input).getBytes(StandardCharsets.UTF_8));
            return HexFormat.of().formatHex(digest);
        } catch (Exception e) {
            throw new IllegalStateException("MD5 不可用", e);
        }
    }

    public boolean alreadyProcessed(String key) {
        return redisson.getBucket(PREFIX + key).isExists();
    }

    public void markProcessed(String key) {
        RBucket<String> bucket = redisson.getBucket(PREFIX + key);
        bucket.set("1", TTL);
    }
}
```

- [ ] **Step 4: 跑 guard 测试确认通过**

Run: `mvn -q -pl agentflow-worker -am test -Dtest=IdempotencyGuardTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 2 PASS(需 Redis 运行)。

- [ ] **Step 5: 写 listener 幂等测试(纯 Mockito,mock guard)**

`SubtaskListenerIdempotencyTest.java`:

```java
package com.agentflow.worker.listener;

import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.common.mq.Topics;
import com.agentflow.worker.idempotency.IdempotencyGuard;
import com.agentflow.worker.processor.EchoProcessor;
import com.agentflow.worker.retry.RetryRouter;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.KafkaTemplate;

import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

class SubtaskListenerIdempotencyTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, Object> template = mock(KafkaTemplate.class);
    private final EchoProcessor processor = spy(new EchoProcessor());
    private final IdempotencyGuard guard = mock(IdempotencyGuard.class);
    private final RetryRouter retryRouter = mock(RetryRouter.class);
    private final SubtaskListener listener =
            new SubtaskListener(processor, template, guard, retryRouter);

    @Test
    void skipsProcessingWhenAlreadyProcessed() throws Exception {
        when(guard.key(any(), any())).thenReturn("k");
        when(guard.alreadyProcessed("k")).thenReturn(true);
        SubtaskMessage msg = new SubtaskMessage(1L, 10L, "u", "ECHO_BATCH", 0, "{\"text\":\"a\"}");

        listener.onMessage(msg, 0);

        verify(processor, never()).process(any());     // 不重复干活
        verify(template, never()).send(eq(Topics.RESULT), anyString(), any());
        verify(guard, never()).markProcessed(any());
    }

    @Test
    void processesAndMarksWhenFirstTime() throws Exception {
        when(guard.key(any(), any())).thenReturn("k");
        when(guard.alreadyProcessed("k")).thenReturn(false);
        SubtaskMessage msg = new SubtaskMessage(1L, 10L, "u", "ECHO_BATCH", 0, "{\"text\":\"hi\"}");

        listener.onMessage(msg, 0);

        verify(processor).process("{\"text\":\"hi\"}");
        verify(template).send(eq(Topics.RESULT), eq("10"), any());
        verify(guard).markProcessed("k");
    }
}
```

> `onMessage` 签名将在本任务改为 `onMessage(SubtaskMessage msg, int attempt)`(attempt 由 Task 3 的消息头注入;本任务先加参数,默认逻辑用它路由失败——失败路由的实现在 Task 3,本任务失败分支暂时仍走 `RetryRouter`,见下)。

- [ ] **Step 6: 改 SubtaskListener(幂等 + 失败委托 RetryRouter)**

先在 Task 3 前建一个**最小 `RetryRouter` 占位**不合适(No Placeholders)。因此本任务与 Task 3 的 `RetryRouter` 接口在此**完整定义**,Task 3 只填其内部实现所需的 topic 常量。`RetryRouter` 完整代码见 Task 3 Step 3;本任务实现 `SubtaskListener` 时,`RetryRouter` 作为构造入参注入(Task 3 提供其 @Component 实现)。

> 执行顺序提示:本 plan 的 Task 2 与 Task 3 有双向引用(listener 用 RetryRouter,RetryRouter 用新 topic)。实现时:**先在 Task 2 内创建 `RetryRouter` 的完整实现(下方代码)与 Topics 新常量**,再回到 listener。即把 Task 3 Step 1（Topics 常量）与 Step 3(RetryRouter)提前到本任务内完成,Task 3 只保留"重试延迟消费者 + DLQ 回传失败结果"。

为避免歧义,**本任务实际交付**:Topics 新常量、`RetryRouter`、`RetryProperties`、`SubtaskListener` 改造、幂等。Task 3 交付:`RetryDelayListener` + DLQ 终局(回传失败结果)。

`SubtaskListener.java` 改为:

```java
package com.agentflow.worker.listener;

import com.agentflow.common.mq.ResultMessage;
import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.common.mq.Topics;
import com.agentflow.worker.idempotency.IdempotencyGuard;
import com.agentflow.worker.processor.EchoProcessor;
import com.agentflow.worker.retry.RetryRouter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

/**
 * Worker 消费入口:幂等检查 → 处理 → 成功回传结果+置幂等键;失败交 RetryRouter 走阶梯重试/DLQ。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SubtaskListener {

    private final EchoProcessor processor;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final IdempotencyGuard guard;
    private final RetryRouter retryRouter;

    @KafkaListener(topics = Topics.SUBTASK)
    public void onMessage(SubtaskMessage msg,
                          @Header(name = Topics.RETRY_ATTEMPT_HEADER, required = false)
                          Integer attemptHeader) {
        int attempt = attemptHeader == null ? 0 : attemptHeader;
        String idemKey = guard.key(msg.getSubtaskUuid(), msg.getInputJson());
        if (guard.alreadyProcessed(idemKey)) {
            log.info("幂等命中,跳过重复处理 subtaskId={}", msg.getSubtaskId());
            return;
        }
        try {
            String output = processor.process(msg.getInputJson());
            guard.markProcessed(idemKey);   // 仅成功置键
            kafkaTemplate.send(Topics.RESULT, String.valueOf(msg.getSubtaskId()),
                    new ResultMessage(msg.getTaskId(), msg.getSubtaskId(), true, output, null));
        } catch (Exception e) {
            log.warn("子任务处理失败 subtaskId={} attempt={},交重试路由", msg.getSubtaskId(), attempt, e);
            retryRouter.route(msg, attempt, e.getMessage());
        }
    }
}
```

> 上面的 `onMessage` 多了 `attemptHeader` 参数;`SubtaskListenerIdempotencyTest` 调用 `listener.onMessage(msg, 0)` 传原始 int——为兼容,额外提供一个包私有重载 `void onMessage(SubtaskMessage msg, int attempt)` 供测试直调,内部转发到主逻辑,或将测试改为传 `Integer`。实现时选后者:测试直接传 `Integer.valueOf(0)`,无需重载。

- [ ] **Step 7: 跑全部 worker 测试**

Run: `mvn -q -pl agentflow-worker -am test`
Expected: 幂等两测 + 既有 EchoProcessorTest 通过。(既有 `SubtaskListenerTest`(W1–2)构造函数签名已变,需同步更新其构造为 `new SubtaskListener(new EchoProcessor(), template, mock(IdempotencyGuard.class), mock(RetryRouter.class))` 并对 guard 打桩 `alreadyProcessed→false`;把这次更新纳入本步。)

- [ ] **Step 8: Commit**

```bash
git add agentflow-worker
git commit -m "feat: 消费幂等(Redisson+MD5)+ RetryRouter 骨架,失败委托阶梯重试"
git push
```

---

### Task 3: Kafka 自研阶梯延迟重试 + 死信终局(卖点5)

**Files:**
- Modify: `agentflow-common/src/main/java/com/agentflow/common/mq/Topics.java`
- Create: `agentflow-worker/src/main/java/com/agentflow/worker/retry/RetryProperties.java`
- Create: `agentflow-worker/src/main/java/com/agentflow/worker/retry/RetryRouter.java`
- Create: `agentflow-worker/src/main/java/com/agentflow/worker/listener/RetryDelayListener.java`
- Modify: `agentflow-server/src/main/java/com/agentflow/server/config/KafkaTopicConfig.java`
- Modify: `agentflow-worker/src/main/resources/application.yml`
- Test: `agentflow-worker/src/test/java/com/agentflow/worker/retry/RetryRouterTest.java`

**Interfaces:**
- Consumes: Task 2 的 `SubtaskListener` 调用 `RetryRouter.route(SubtaskMessage, int attempt, String errorMsg)`。
- Produces:
  - `Topics.RETRY_5S/RETRY_30S/RETRY_5M/DLQ` 常量;`Topics.RETRY_ATTEMPT_HEADER="x-retry-attempt"`、`Topics.NOT_BEFORE_HEADER="x-not-before"`。
  - `RetryRouter.route(msg, attempt, errorMsg)`:attempt 0→RETRY_5S,1→RETRY_30S,2→RETRY_5M,≥3→DLQ + 回传失败 `ResultMessage`。
  - `RetryDelayListener`:消费三个 retry topic,读 `x-not-before` sleep 到点,重投 `SUBTASK`,`x-retry-attempt=attempt+1`。

- [ ] **Step 1: Topics 增常量**

`Topics.java` 改为:

```java
package com.agentflow.common.mq;

public final class Topics {
    public static final String SUBTASK = "AGENTFLOW_SUBTASK";
    public static final String RESULT = "AGENTFLOW_RESULT";
    public static final String RETRY_5S = "AGENTFLOW_RETRY_5S";
    public static final String RETRY_30S = "AGENTFLOW_RETRY_30S";
    public static final String RETRY_5M = "AGENTFLOW_RETRY_5M";
    public static final String DLQ = "AGENTFLOW_DLQ";

    public static final String RETRY_ATTEMPT_HEADER = "x-retry-attempt";
    public static final String NOT_BEFORE_HEADER = "x-not-before"; // epoch millis

    private Topics() {}
}
```

- [ ] **Step 2: server 端声明新 topic**

`KafkaTopicConfig.java` 追加 4 个 bean(分区 3 副本 1,与主 topic 一致):

```java
    @Bean public NewTopic retry5sTopic()  { return TopicBuilder.name(Topics.RETRY_5S).partitions(3).replicas(1).build(); }
    @Bean public NewTopic retry30sTopic() { return TopicBuilder.name(Topics.RETRY_30S).partitions(3).replicas(1).build(); }
    @Bean public NewTopic retry5mTopic()  { return TopicBuilder.name(Topics.RETRY_5M).partitions(3).replicas(1).build(); }
    @Bean public NewTopic dlqTopic()      { return TopicBuilder.name(Topics.DLQ).partitions(3).replicas(1).build(); }
```

- [ ] **Step 3: RetryProperties(档位常量,可属性覆盖)**

`RetryProperties.java`:

```java
package com.agentflow.worker.retry;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** 阶梯延迟档位(毫秒),默认 5s/30s/5m;测试用属性覆盖为短档。最多 3 次重试。 */
@Getter
@Component
public class RetryProperties {
    @Value("${agentflow.retry.delay-5s-ms:5000}")   private long delay5s;
    @Value("${agentflow.retry.delay-30s-ms:30000}") private long delay30s;
    @Value("${agentflow.retry.delay-5m-ms:300000}") private long delay5m;
    public static final int MAX_ATTEMPTS = 3;
}
```

- [ ] **Step 4: 写 RetryRouter 单测(纯 Mockito)**

`RetryRouterTest.java`:

```java
package com.agentflow.worker.retry;

import com.agentflow.common.mq.ResultMessage;
import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.common.mq.Topics;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class RetryRouterTest {

    @SuppressWarnings("unchecked")
    private final KafkaTemplate<String, Object> template = mock(KafkaTemplate.class);
    private final RetryProperties props = new RetryProperties();
    private final RetryRouter router = new RetryRouter(template, props);

    private final SubtaskMessage msg =
            new SubtaskMessage(1L, 10L, "u", "ECHO_BATCH", 0, "{\"text\":\"a\"}");

    @Test
    void attempt0GoesToRetry5s() {
        router.route(msg, 0, "boom");
        ArgumentCaptor<ProducerRecord<String, Object>> cap = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(cap.capture());
        assertEquals(Topics.RETRY_5S, cap.getValue().topic());
        assertNotNull(cap.getValue().headers().lastHeader(Topics.NOT_BEFORE_HEADER));
    }

    @Test
    void attempt2GoesToRetry5m() {
        router.route(msg, 2, "boom");
        ArgumentCaptor<ProducerRecord<String, Object>> cap = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(template).send(cap.capture());
        assertEquals(Topics.RETRY_5M, cap.getValue().topic());
    }

    @Test
    void attempt3ExhaustsToDlqAndSendsFailedResult() {
        router.route(msg, 3, "boom");
        // 一条进 DLQ(ProducerRecord),一条失败 ResultMessage 进 RESULT(topic,key,value)
        verify(template).send(argThat((ProducerRecord<String, Object> r) -> Topics.DLQ.equals(r.topic())));
        ArgumentCaptor<Object> resultCap = ArgumentCaptor.forClass(Object.class);
        verify(template).send(eq(Topics.RESULT), eq("10"), resultCap.capture());
        ResultMessage rm = (ResultMessage) resultCap.getValue();
        assertFalse(rm.isSuccess());
        assertEquals("boom", rm.getErrorMsg());
    }
}
```

- [ ] **Step 5: 实现 RetryRouter**

`RetryRouter.java`:

```java
package com.agentflow.worker.retry;

import com.agentflow.common.mq.ResultMessage;
import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.common.mq.Topics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * 失败路由:按 attempt 阶梯延迟重试,超 MAX_ATTEMPTS 进 DLQ 并回传失败结果(任务得以终结为 FAILED)。
 * attempt: 0→RETRY_5S, 1→RETRY_30S, 2→RETRY_5M, >=3→DLQ。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RetryRouter {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final RetryProperties props;

    public void route(SubtaskMessage msg, int attempt, String errorMsg) {
        String key = String.valueOf(msg.getSubtaskId());
        if (attempt >= RetryProperties.MAX_ATTEMPTS) {
            // 死信 + 回传失败结果(让 server 落定该子任务为 FAILED)
            kafkaTemplate.send(new ProducerRecord<>(Topics.DLQ, key, msg));
            kafkaTemplate.send(Topics.RESULT, key,
                    new ResultMessage(msg.getTaskId(), msg.getSubtaskId(), false, null, errorMsg));
            log.error("子任务重试耗尽,进 DLQ subtaskId={}", msg.getSubtaskId());
            return;
        }
        String topic;
        long delayMs;
        switch (attempt) {
            case 0 -> { topic = Topics.RETRY_5S;  delayMs = props.getDelay5s(); }
            case 1 -> { topic = Topics.RETRY_30S; delayMs = props.getDelay30s(); }
            default -> { topic = Topics.RETRY_5M; delayMs = props.getDelay5m(); }
        }
        long notBefore = System.currentTimeMillis() + delayMs;
        ProducerRecord<String, Object> rec = new ProducerRecord<>(topic, key, msg);
        rec.headers().add(new RecordHeader(Topics.NOT_BEFORE_HEADER,
                String.valueOf(notBefore).getBytes(StandardCharsets.UTF_8)));
        kafkaTemplate.send(rec);
        log.info("子任务转重试 subtaskId={} attempt={} topic={} delayMs={}",
                msg.getSubtaskId(), attempt, topic, delayMs);
    }
}
```

- [ ] **Step 6: worker consumer 配置(sleep 安全)**

`agentflow-worker/src/main/resources/application.yml` 的 `spring.kafka.consumer` 下追加(防 5m sleep 触发 rebalance):

```yaml
      max-poll-records: 1
      properties:
        spring.json.trusted.packages: com.agentflow.common.mq
        max.poll.interval.ms: 600000
```

> 注意:`properties` 键已存在(trusted.packages),合并进同一块,勿重复键。

- [ ] **Step 7: 实现 RetryDelayListener(三 topic 延迟消费者)**

`RetryDelayListener.java`:

```java
package com.agentflow.worker.listener;

import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.common.mq.Topics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * 重试延迟消费者:读 x-not-before,sleep 到点,重投主 topic,attempt+1。
 * max.poll.records=1 + max.poll.interval.ms=600000 保证单条 sleep 不触发 rebalance。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class RetryDelayListener {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    @KafkaListener(topics = {Topics.RETRY_5S, Topics.RETRY_30S, Topics.RETRY_5M})
    public void onRetry(SubtaskMessage msg,
                        @org.springframework.messaging.handler.annotation.Header(
                                name = Topics.NOT_BEFORE_HEADER, required = false) Long notBefore,
                        @org.springframework.messaging.handler.annotation.Header(
                                name = Topics.RETRY_ATTEMPT_HEADER, required = false) Integer attemptHeader)
            throws InterruptedException {
        int attempt = attemptHeader == null ? 0 : attemptHeader;
        long waitMs = notBefore == null ? 0 : notBefore - System.currentTimeMillis();
        if (waitMs > 0) {
            Thread.sleep(waitMs);
        }
        ProducerRecord<String, Object> rec = new ProducerRecord<>(
                Topics.SUBTASK, String.valueOf(msg.getSubtaskId()), msg);
        rec.headers().add(new RecordHeader(Topics.RETRY_ATTEMPT_HEADER,
                String.valueOf(attempt + 1).getBytes(StandardCharsets.UTF_8)));
        kafkaTemplate.send(rec);
        log.info("重试到点,重投主 topic subtaskId={} 新 attempt={}", msg.getSubtaskId(), attempt + 1);
    }
}
```

> **实现注意**:上面第一个 import 行 `org.springframework...Header.class` 是笔误占位——删掉它;正文用全限定 `@org.springframework.messaging.handler.annotation.Header(...)` 取值。header 值经 spring-kafka 反序列化:`x-retry-attempt`/`x-not-before` 以字符串字节写入,接收端声明为 `Integer`/`Long` 时 spring-kafka 默认按字符串→数值转换可能不生效——**稳妥做法**:接收端声明为 `byte[]` 或 `String`,再手动 `Long.parseLong(new String(bytes))`。实现时按 String 接收并手动解析,避免 header 类型转换踩坑(记入报告)。

- [ ] **Step 8: 跑 RetryRouter 测试 + 全 worker 回归**

Run: `mvn -q -pl agentflow-worker -am test -Dtest=RetryRouterTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 3 PASS。
Run: `mvn -q -pl agentflow-worker -am test`
Expected: 全绿。

- [ ] **Step 9: 集成验证(短档位,手动)**

用测试属性把档位改短(如 `-Dagentflow.retry.delay-5s-ms=1000`),或写一个 `@SpringBootTest` 让 `EchoProcessor` 对特定输入抛异常,观察消息经 RETRY_5S→...→DLQ。此步可作为 Task 8 容错演练的一部分,若此处只做单测则在报告里说明延迟链路留待 Task 8 端到端验证。

- [ ] **Step 10: Commit**

```bash
git add agentflow-common agentflow-worker agentflow-server
git commit -m "feat: Kafka 自研阶梯延迟重试(5s/30s/5m)+ DLQ 终局(卖点5)"
git push
```

---

### Task 4: 超时兜底(@Scheduled 扫描静默失败)

**Files:**
- Modify: `docker/mysql/init.sql`(subtask 增 `redispatch_count`)
- Modify: `agentflow-server/src/main/java/com/agentflow/server/entity/SubtaskEntity.java`
- Modify: `agentflow-server/src/main/java/com/agentflow/server/mapper/SubtaskMapper.java`
- Create: `agentflow-server/src/main/java/com/agentflow/server/service/TimeoutSweepService.java`
- Create: `agentflow-server/src/main/java/com/agentflow/server/scheduler/TimeoutScheduler.java`
- Modify: `agentflow-server/src/main/java/com/agentflow/server/ServerApplication.java`(`@EnableScheduling`)
- Test: `agentflow-server/src/test/java/com/agentflow/server/service/TimeoutSweepServiceTest.java`

**Interfaces:**
- Consumes: Task 1 的 `ResultHandleService`(通过状态机复用);W1–2 的 `SubtaskMapper.casStatus`、`TaskMapper.incrementFailed`。
- Produces: `TimeoutSweepService.sweep()`——扫描 `DISPATCHED` 且 `updated_at < now-阈值` 的子任务:`redispatch_count < 上限` → 重投主 topic + `redispatch_count+1`;否则 → CAS DISPATCHED→FAILED + 计数 + finalize。

- [ ] **Step 1: 加列(init.sql + 运行库 ALTER)**

`docker/mysql/init.sql` 的 `subtask` 表在 `error_msg` 后加一列:

```sql
  redispatch_count INT NOT NULL DEFAULT 0,
```

运行中的开发库需同步(新卷才会跑 init.sql):
Run: `export PATH="$HOME/.orbstack/bin:$PATH"; docker exec agentflow-mysql mysql -uroot -pagentflow123 -e "ALTER TABLE subtask ADD COLUMN redispatch_count INT NOT NULL DEFAULT 0" agentflow`
Expected: 成功(无输出)。

`SubtaskEntity.java` 加字段:

```java
    private Integer redispatchCount;
```

- [ ] **Step 2: SubtaskMapper 增查询与自增**

`SubtaskMapper.java` 增:

```java
    /** 卡在 DISPATCHED 超过 cutoff 的子任务(静默失败候选)。 */
    @Select("SELECT * FROM subtask WHERE status = 'DISPATCHED' AND updated_at < #{cutoff}")
    java.util.List<SubtaskEntity> findStuckDispatched(@Param("cutoff") java.time.LocalDateTime cutoff);

    @Update("UPDATE subtask SET redispatch_count = redispatch_count + 1 WHERE id = #{id}")
    int incrementRedispatch(@Param("id") Long id);
```

(import 增 `org.apache.ibatis.annotations.Select`。)

- [ ] **Step 3: 写 TimeoutSweepService 集成测试**

`TimeoutSweepServiceTest.java`(`@SpringBootTest`,`@MockBean KafkaTemplate`;直接造过期 DISPATCHED 行):

```java
package com.agentflow.server.service;

import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.LocalDateTime;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class TimeoutSweepServiceTest {

    @Autowired private TimeoutSweepService sweep;
    @Autowired private TaskMapper taskMapper;
    @Autowired private SubtaskMapper subtaskMapper;
    @MockBean private KafkaTemplate<String, Object> kafkaTemplate;

    private Long taskId, subId;

    private void seedStuck(int redispatchCount) {
        TaskEntity t = new TaskEntity();
        t.setTaskUuid(UUID.randomUUID().toString());
        t.setType("ECHO_BATCH"); t.setStatus("RUNNING");
        t.setSubtaskTotal(1); t.setSubtaskDone(0); t.setSubtaskFailed(0);
        taskMapper.insert(t); taskId = t.getId();
        SubtaskEntity s = new SubtaskEntity();
        s.setSubtaskUuid(UUID.randomUUID().toString());
        s.setTaskId(taskId); s.setSeq(0); s.setStatus("DISPATCHED");
        s.setInput("{\"text\":\"x\"}"); s.setRedispatchCount(redispatchCount);
        subtaskMapper.insert(s); subId = s.getId();
        // 强制 updated_at 过期(绕过 ON UPDATE 自动刷新)
        subtaskMapper.setUpdatedAt(subId, LocalDateTime.now().minusMinutes(10));
    }

    @AfterEach
    void cleanup() {
        if (subId != null) subtaskMapper.deleteById(subId);
        if (taskId != null) taskMapper.deleteById(taskId);
    }

    @Test
    void redispatchesWhenUnderLimit() {
        seedStuck(0);
        sweep.sweep();
        SubtaskEntity s = subtaskMapper.selectById(subId);
        assertEquals("DISPATCHED", s.getStatus());       // 仍在跑
        assertEquals(1, s.getRedispatchCount());          // 重投计数 +1
    }

    @Test
    void failsWhenOverLimit() {
        seedStuck(3);                                     // 已达上限(MAX_REDISPATCH=3)
        sweep.sweep();
        assertEquals("FAILED", subtaskMapper.selectById(subId).getStatus());
        assertEquals("FAILED", taskMapper.selectById(taskId).getStatus()); // 单子任务全败→FAILED
    }
}
```

> 需给 `SubtaskMapper` 补一个测试辅助更新:`@Update("UPDATE subtask SET updated_at = #{ts} WHERE id = #{id}") int setUpdatedAt(@Param("id") Long id, @Param("ts") LocalDateTime ts);`(生产不用,仅测试造过期数据)。

- [ ] **Step 4: 实现 TimeoutSweepService**

`TimeoutSweepService.java`:

```java
package com.agentflow.server.service;

import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.common.mq.Topics;
import com.agentflow.common.state.SubtaskStatus;
import com.agentflow.common.state.TaskStatus;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 超时兜底:扫描卡在 DISPATCHED 的子任务(worker 死/消息丢,无任何回传)。
 * 未超上限 → 重投主 topic;超上限 → 落定 FAILED 并推进任务终态。
 * 与「处理异常重试」(worker 侧 RetryRouter)触发源不同,不重叠。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TimeoutSweepService {

    static final int MAX_REDISPATCH = 3;

    private final SubtaskMapper subtaskMapper;
    private final TaskMapper taskMapper;
    private final TaskStateMachine stateMachine;
    private final ResultHandleService resultHandleService; // 复用 finalize(经状态机)
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${agentflow.timeout.stuck-seconds:60}")
    private long stuckSeconds;

    @Transactional
    public void sweep() {
        LocalDateTime cutoff = LocalDateTime.now().minusSeconds(stuckSeconds);
        List<SubtaskEntity> stuck = subtaskMapper.findStuckDispatched(cutoff);
        for (SubtaskEntity s : stuck) {
            if (s.getRedispatchCount() < MAX_REDISPATCH) {
                subtaskMapper.incrementRedispatch(s.getId());
                // touch updated_at:重投后重新计时(mapper 的自增会触发 ON UPDATE 刷新 updated_at)
                kafkaTemplate.send(Topics.SUBTASK, String.valueOf(s.getId()),
                        new SubtaskMessage(s.getTaskId(), s.getId(), s.getSubtaskUuid(),
                                "ECHO_BATCH", s.getSeq(), s.getInput()));
                log.warn("超时重投 subtaskId={} redispatch={}", s.getId(), s.getRedispatchCount() + 1);
            } else {
                if (stateMachine.transitionSubtask(s.getId(),
                        SubtaskStatus.DISPATCHED, SubtaskStatus.FAILED)) {
                    s.setStatus(SubtaskStatus.FAILED.name());
                    s.setErrorMsg("超时未回传,重投耗尽");
                    subtaskMapper.updateById(s);
                    taskMapper.incrementFailed(s.getTaskId());
                    // 复用结果处理的 finalize:构造一条失败结果直接落定
                    // 简化:直接调用私有 finalize 不可行,改为走 handle 幂等入口——
                    // 但子任务已 FAILED,handle 的 CAS 会失败。故此处内联 finalize 逻辑:
                    finalizeTask(s.getTaskId());
                    log.error("超时失败落定 subtaskId={}", s.getId());
                }
            }
        }
    }

    /** 与 ResultHandleService.finalizeIfAllSettled 同逻辑;抽取为可复用。 */
    private void finalizeTask(Long taskId) {
        var task = taskMapper.selectById(taskId);
        if (task.getSubtaskDone() + task.getSubtaskFailed() < task.getSubtaskTotal()) return;
        TaskStatus terminal = task.getSubtaskFailed() == 0 ? TaskStatus.COMPLETED
                : task.getSubtaskDone() == 0 ? TaskStatus.FAILED : TaskStatus.PARTIAL_FAILED;
        if (stateMachine.transitionTask(taskId, TaskStatus.RUNNING, terminal)) {
            resultHandleService.aggregatePublic(task, terminal);
        }
    }
}
```

> **重构提示(DRY)**:`ResultHandleService` 的 `finalizeIfAllSettled` + `aggregate` 与此处重复。实现时把 `ResultHandleService.aggregate` 提升为 `public void aggregatePublic(TaskEntity, TaskStatus)`(或抽一个 `TaskFinalizer` 组件供两处共用)。**推荐**:新建 `TaskFinalizer` 组件封装 finalize+aggregate,`ResultHandleService` 与 `TimeoutSweepService` 都注入它。若抽取,Task 1 的 `handle` 也改为调用 `TaskFinalizer`。实现者择一,并在报告说明;测试须覆盖两条调用路径。

- [ ] **Step 5: 定时器 + @EnableScheduling**

`TimeoutScheduler.java`:

```java
package com.agentflow.server.scheduler;

import com.agentflow.server.service.TimeoutSweepService;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class TimeoutScheduler {
    private final TimeoutSweepService sweep;

    @Scheduled(fixedDelayString = "${agentflow.timeout.sweep-interval-ms:30000}")
    public void run() { sweep.sweep(); }
}
```

`ServerApplication.java` 类上加 `@EnableScheduling`(import `org.springframework.scheduling.annotation.EnableScheduling`)。

- [ ] **Step 6: 跑测试 + 回归**

Run: `mvn -q -pl agentflow-server -am test -Dtest=TimeoutSweepServiceTest -Dsurefire.failIfNoSpecifiedTests=false`
Expected: 2 PASS。
Run: `mvn -q test` → 全绿。

- [ ] **Step 7: Commit**

```bash
git add agentflow-server docker/mysql/init.sql
git commit -m "feat: 超时兜底 @Scheduled 扫描——卡死子任务重投/失败落定"
git push
```

---

### Task 5: DLQ 恢复重放

**Files:**
- Create: `agentflow-server/src/main/java/com/agentflow/server/service/DlqRecoveryService.java`
- Create: `agentflow-server/src/main/java/com/agentflow/server/controller/DlqController.java`
- Modify: `agentflow-server/src/main/resources/application.yml`(server 也需消费 DLQ 的 trusted packages,已有)
- Test: `agentflow-server/src/test/java/com/agentflow/server/service/DlqRecoveryServiceTest.java`

**Interfaces:**
- Produces: `DlqRecoveryService.replay(Long subtaskId)`——把一个已 FAILED、且其任务已终态的子任务**重置**为 PENDING(task 计数回退、状态回 RUNNING),再重投主 topic(attempt 归零)。`POST /api/v1/dlq/replay/{subtaskId}` 触发。

> **说明**:W3 的 DLQ 恢复采用「按 subtaskId 显式重放」——运营者从 DLQ/告警看到失败,调接口恢复。自动消费 DLQ topic 做审计留痕属子计划二(Run Trace),本任务不做 DLQ 的自动消费,只做「重置+重投」恢复动作。

- [ ] **Step 1: 写恢复测试**

`DlqRecoveryServiceTest.java`(`@SpringBootTest`,`@MockBean KafkaTemplate`;造一个 FAILED 子任务 + FAILED 任务):

```java
package com.agentflow.server.service;

import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.verify;

@SpringBootTest
class DlqRecoveryServiceTest {

    @Autowired private DlqRecoveryService recovery;
    @Autowired private TaskMapper taskMapper;
    @Autowired private SubtaskMapper subtaskMapper;
    @MockBean private KafkaTemplate<String, Object> kafkaTemplate;

    private Long taskId, subId;

    @AfterEach
    void cleanup() {
        if (subId != null) subtaskMapper.deleteById(subId);
        if (taskId != null) taskMapper.deleteById(taskId);
    }

    @Test
    void replayResetsSubtaskAndTaskThenRedispatches() {
        TaskEntity t = new TaskEntity();
        t.setTaskUuid(UUID.randomUUID().toString());
        t.setType("ECHO_BATCH"); t.setStatus("FAILED");   // 已终态
        t.setSubtaskTotal(1); t.setSubtaskDone(0); t.setSubtaskFailed(1);
        taskMapper.insert(t); taskId = t.getId();
        SubtaskEntity s = new SubtaskEntity();
        s.setSubtaskUuid(UUID.randomUUID().toString());
        s.setTaskId(taskId); s.setSeq(0); s.setStatus("FAILED");
        s.setInput("{\"text\":\"x\"}"); s.setErrorMsg("boom"); s.setRedispatchCount(0);
        subtaskMapper.insert(s); subId = s.getId();

        recovery.replay(subId);

        SubtaskEntity s2 = subtaskMapper.selectById(subId);
        assertEquals("PENDING", s2.getStatus());          // 重置
        TaskEntity t2 = taskMapper.selectById(taskId);
        assertEquals("RUNNING", t2.getStatus());          // 任务复活
        assertEquals(0, t2.getSubtaskFailed());           // 失败计数回退
        verify(kafkaTemplate).send(eq(com.agentflow.common.mq.Topics.SUBTASK),
                eq(String.valueOf(subId)), any());        // 重投主 topic
    }
}
```

- [ ] **Step 2: 实现 DlqRecoveryService**

`DlqRecoveryService.java`:

```java
package com.agentflow.server.service;

import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.common.mq.Topics;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * DLQ 恢复:把已 FAILED 的子任务重置为 PENDING,回退任务失败计数并复活为 RUNNING,再重投主 topic。
 * 明确"谁来恢复":运营者按 subtaskId 显式触发。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class DlqRecoveryService {

    private final SubtaskMapper subtaskMapper;
    private final TaskMapper taskMapper;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Transactional
    public void replay(Long subtaskId) {
        SubtaskEntity s = subtaskMapper.selectById(subtaskId);
        if (s == null || !"FAILED".equals(s.getStatus())) {
            throw new IllegalArgumentException("子任务不存在或非 FAILED,不可恢复: " + subtaskId);
        }
        // 1. 子任务 FAILED → PENDING,清错误,重投计数归零
        s.setStatus("PENDING");
        s.setErrorMsg(null);
        s.setRedispatchCount(0);
        subtaskMapper.updateById(s);
        // 2. 任务失败计数回退 + 复活为 RUNNING
        TaskEntity t = taskMapper.selectById(s.getTaskId());
        taskMapper.decrementFailed(t.getId());
        t.setStatus("RUNNING");
        t.setResult(null);
        taskMapper.updateById(t);
        // 3. 重投主 topic(attempt 归零,不带 header)
        kafkaTemplate.send(Topics.SUBTASK, String.valueOf(s.getId()),
                new SubtaskMessage(t.getId(), s.getId(), s.getSubtaskUuid(),
                        t.getType(), s.getSeq(), s.getInput()));
        log.info("DLQ 恢复:子任务重置并重投 subtaskId={} taskId={}", subtaskId, t.getId());
    }
}
```

给 `TaskMapper` 补:

```java
    @Update("UPDATE task SET subtask_failed = subtask_failed - 1 WHERE id = #{id}")
    int decrementFailed(@Param("id") Long id);
```

- [ ] **Step 3: Controller**

`DlqController.java`:

```java
package com.agentflow.server.controller;

import com.agentflow.server.service.DlqRecoveryService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/dlq")
@RequiredArgsConstructor
public class DlqController {

    private final DlqRecoveryService recovery;

    @PostMapping("/replay/{subtaskId}")
    public ResponseEntity<Map<String, String>> replay(@PathVariable("subtaskId") Long subtaskId) {
        recovery.replay(subtaskId);
        return ResponseEntity.accepted().body(Map.of("replayed", String.valueOf(subtaskId)));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, String>> bad(IllegalArgumentException e) {
        return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
    }
}
```

- [ ] **Step 4: 跑测试 + 回归**

Run: `mvn -q -pl agentflow-server -am test -Dtest=DlqRecoveryServiceTest -Dsurefire.failIfNoSpecifiedTests=false` → PASS。
Run: `mvn -q test` → 全绿。

- [ ] **Step 5: Commit**

```bash
git add agentflow-server
git commit -m "feat: DLQ 恢复重放——重置子任务+复活任务+重投主 topic"
git push
```

---

### Task 6: 优雅停机 + 多 Worker 文档

**Files:**
- Modify: `agentflow-worker/src/main/resources/application.yml`(优雅停机)
- Modify: `README.md`(多实例运行说明)
- Test:(无新单测;优雅停机由配置保证,行为在 Task 8 演练)

**Interfaces:** 无新代码接口;确保 worker 关闭时处理完在途消息、提交 offset。

- [ ] **Step 1: 开启优雅停机配置**

`agentflow-worker/src/main/resources/application.yml` 的 `spring:` 下加:

```yaml
  lifecycle:
    timeout-per-shutdown-phase: 30s
server:
  shutdown: graceful
```

`spring.kafka.listener` 下确保容器随上下文优雅停止(spring-kafka 默认在上下文关闭时停止容器、提交 offset;无需额外代码)。追加显式项:

```yaml
  kafka:
    listener:
      immediate-stop: false
```

> worker 无 web server,`server.shutdown` 无效但无害;关键是 `spring.lifecycle.timeout-per-shutdown-phase` 给监听容器留出处理完当前 record 的时间。配合 Task 2 幂等:即便停机后消息被别的实例重投,也不会重复干活。

- [ ] **Step 2: README 加多实例说明**

`README.md` 追加一节:

```markdown
## 多 Worker 水平扩展

主 topic 3 分区,最多 3 个 Worker 并行(同消费组自动 rebalance):

    # 终端各起一个,同 group 自动分走分区
    mvn -q -pl agentflow-worker -am spring-boot:run
    mvn -q -pl agentflow-worker -am spring-boot:run
    mvn -q -pl agentflow-worker -am spring-boot:run

优雅停机:`Ctrl-C` 后 Worker 处理完在途子任务、提交 offset 再退出,配合幂等去重保证不丢不重。
(1→3 Worker 吞吐提升比在 W8 压测量化。)
```

- [ ] **Step 3: 编译验证 + Commit**

Run: `mvn -q -pl agentflow-worker -am compile` → BUILD SUCCESS。

```bash
git add agentflow-worker/src/main/resources/application.yml README.md
git commit -m "feat: Worker 优雅停机配置 + 多实例水平扩展文档"
git push
```

---

### Task 7: 端到端容错演练 + 里程碑验收

**Files:**
- Create: `scripts/fault-drill.sh`
- Modify: `docs/backlog.md`(勾掉已完成项,记录新发现)
- Modify: `README.md`(路线图勾掉 W3 核心)

**Interfaces:** 消费全部前序任务。

- [ ] **Step 1: 写容错演练脚本**

`scripts/fault-drill.sh`:

```bash
#!/usr/bin/env bash
# 容错演练:前置 docker compose up -d + server 起 + 至少 1 个 worker 起。
# 场景:提交任务 → 处理中途 kill 一个 worker → 验证任务最终仍 COMPLETED(消息重投+幂等,零丢失)。
set -euo pipefail
BASE="http://localhost:8080/api/v1/tasks"

echo "==> 提交 5 子任务"
UUID=$(curl -s -X POST "$BASE" -H 'Content-Type: application/json' \
  -d '{"type":"ECHO_BATCH","payload":{"items":["a","b","c","d","e"]}}' \
  | python3 -c 'import sys,json;print(json.load(sys.stdin)["taskUuid"])')
echo "    taskUuid=$UUID"

echo "==> 立即 kill 一个 worker(制造在途中断),你可在另一终端 Ctrl-C 一个 worker 实例"
echo "==> 轮询直到 COMPLETED(消息应被其它实例/重投接手)"
for i in $(seq 1 40); do
  ST=$(curl -s "$BASE/$UUID" | python3 -c 'import sys,json;print(json.load(sys.stdin)["status"])')
  echo "    [$i] $ST"
  [ "$ST" = "COMPLETED" ] && { echo "✅ 零丢失:kill worker 后任务仍完成"; exit 0; }
  [ "$ST" = "FAILED" ] && { echo "❌ 任务失败"; curl -s "$BASE/$UUID" | python3 -m json.tool; exit 1; }
  sleep 3
done
echo "❌ 超时未完成"; exit 1
```

- [ ] **Step 2: 手动跑通三个场景并记录**

前置:`docker compose up -d`,server 起,起 2 个 worker。
1. **kill worker 零丢失**:`chmod +x scripts/fault-drill.sh && ./scripts/fault-drill.sh`,期间 Ctrl-C 掉一个 worker → 期望 `✅`。
2. **重试→DLQ→恢复**:临时让 `EchoProcessor` 对某输入抛异常(或用短延迟档位属性),提交含该输入的任务 → 观察 worker 日志经 RETRY_5S→30S→5M→DLQ,任务落 FAILED/PARTIAL_FAILED → 调 `POST /api/v1/dlq/replay/{subtaskId}` → 任务复活并完成。
3. **超时兜底**:起 server 但**不起 worker**,提交任务 → 子任务卡 DISPATCHED → 等超时扫描重投(仍无 worker 则最终 FAILED),验证不会永远 RUNNING。
把三个场景的实际输出贴进报告。

- [ ] **Step 3: 更新 backlog 与路线图**

`docs/backlog.md`:勾掉"@Transactional 崩溃恢复"、把 W3 已交付项标记完成;记录本轮新发现(如 header 类型转换坑、finalize 抽取 TaskFinalizer 的决定)。
`README.md` 路线图:`- [x] W3–4 可靠性核心:幂等、Kafka 自研重试/死信+恢复、超时兜底、优雅停机`。

- [ ] **Step 4: 里程碑自检(能脱稿讲清)**

在报告回答:
1. 幂等键为何只在成功后置?与合法重试如何不冲突?
2. Kafka 无原生延迟,你怎么实现阶梯延迟?为何 `max.poll.interval.ms` 要调大?pause/seek 方案相比 sleep 的取舍?
3. `@Transactional` 修的是什么撕裂?超时扫描又兜什么?两者为何都要?
4. DLQ 恢复为何要"重置子任务+回退计数+复活任务",而不是简单重投消息?

- [ ] **Step 5: Commit**

```bash
git add scripts/fault-drill.sh docs/backlog.md README.md
git commit -m "feat: 容错演练脚本 + W3 可靠性核心里程碑验收"
git push
```

---

## 后续(本子计划之外)

- **子计划二**:Run Trace(MongoDB 单机)+ 统一副作用 Policy 层(含 DLQ 自动消费审计留痕)。
- W8 压测:1→3 Worker 吞吐比、幂等拦截重复调用比例、kill worker 零丢失率等量化数字。
- W9:ZooKeeper 选主/注册(可选);Kafka UI。

---

## Self-Review(计划自查)

**Spec 覆盖**:①崩溃修复=Task1;②幂等=Task2;③重试/死信/恢复=Task3+Task5;④超时=Task4;⑤优雅停机+多Worker=Task6;容错演练=Task7。全覆盖。
**类型一致**:`RetryRouter.route(SubtaskMessage,int,String)`、`IdempotencyGuard.{key,alreadyProcessed,markProcessed}`、`Topics.*` 常量、`SubtaskMapper.{findStuckDispatched,incrementRedispatch,setUpdatedAt}`、`TaskMapper.decrementFailed`、`redispatchCount` 字段——前后引用一致。
**已知需实现者决断并在报告说明的点**:(a) header 值按 String 接收手动解析(避免类型转换坑);(b) finalize/aggregate 抽取为共享 `TaskFinalizer`(DRY),Task1/4 两处复用;(c) `RetryDelayListener` 代码里那行笔误 import 删除。这些在对应任务已明确指示,非占位。
