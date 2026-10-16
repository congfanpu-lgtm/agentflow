package com.agentflow.server.service;

import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.common.mq.Topics;
import com.agentflow.common.state.SubtaskStatus;
import com.agentflow.common.trace.TraceStage;
import com.agentflow.server.coordinator.StageAdvancer;
import com.agentflow.server.coordinator.SubtaskDispatcher;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import com.agentflow.server.trace.TraceEmitter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

/**
 * 超时兜底:扫描卡在 DISPATCHED 的子任务(worker 死/消息丢,无任何回传)。
 * 未超上限 → 重投主 topic;超上限 → 落定 FAILED 并推进任务终态。
 * <p>
 * 与「处理异常重试」(worker 侧 RetryRouter 的重试阶梯,约 5s+30s+5m ≈ 335s)按【触发源】区分,
 * 二者不是同一回事:重试阶梯处理的是"处理异常"(worker 收到消息但处理失败,topic 路由到延迟重试
 * topic,全程不落库、不更新 updated_at);本服务处理的是"静默失败"(worker 从未处理——进程死亡
 * 或消息丢失,既无结果回传也无重试活动)。{@code stuck-seconds} 默认值刻意设置在整条重试阶梯
 * 总时长之上(留有余量),使兜底只对"确实静默"的子任务生效,不会在阶梯进行中把它误判为卡死、
 * 抢先重投/落定,从而打断正在进行的重试。
 * 落定后统一经 {@link StageAdvancer}(推进下一阶段或判终态),与 {@link ResultHandleService} 共用(DRY)。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TimeoutSweepService {

    static final int MAX_REDISPATCH = 3;

    private final SubtaskMapper subtaskMapper;
    private final TaskMapper taskMapper;
    private final TaskStateMachine stateMachine;
    private final StageAdvancer stageAdvancer;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TraceEmitter traceEmitter;
    private final SubtaskDispatcher dispatcher;

    // 默认 600s:高于重试阶梯全程耗时(约 5s+30s+5m ≈ 335s)且留有余量,避免抢占仍在阶梯中的子任务。
    @Value("${agentflow.timeout.stuck-seconds:600}")
    private long stuckSeconds;

    // PENDING 补发阈值:正常提交/阶段推进从插入到发送只有毫秒级,60s 仍是 PENDING 说明那次发送失败了
    @Value("${agentflow.timeout.pending-seconds:60}")
    private long pendingSeconds;

    // READ_COMMITTED:任务计数行锁让落定串行化,但 MySQL 默认 REPEATABLE READ 会让阶段推进读到事务开始时的旧快照,
    // 漏掉并发提交的结果(见 StageAdvanceIsolationTest)。每条语句读最新已提交数据才能配合行锁得出正确的下一阶段。
    @Transactional(isolation = Isolation.READ_COMMITTED)
    public void sweep() {
        // 1) 补发:发送 MQ 失败而停在 PENDING 的子任务(提交、阶段推进都在 afterCommit 发送,失败只记日志)
        for (Long id : subtaskMapper.findTasksWithStalePending(LocalDateTime.now().minusSeconds(pendingSeconds))) {
            log.warn("补发停在 PENDING 的子任务 taskId={}", id);
            dispatcher.dispatchPending(taskMapper.selectById(id));
        }
        // 2) 静默失败:卡在 DISPATCHED 的子任务
        LocalDateTime cutoff = LocalDateTime.now().minusSeconds(stuckSeconds);
        List<SubtaskEntity> stuck = subtaskMapper.findStuckDispatched(cutoff);
        for (SubtaskEntity s : stuck) {
            if (s.getRedispatchCount() < MAX_REDISPATCH) {
                subtaskMapper.incrementRedispatch(s.getId());
                // 重投后 incrementRedispatch 的 ON UPDATE 会刷新 updated_at,重新计时。
                // 修 backlog T4b:type 取自任务而非硬编码 ECHO_BATCH,否则多任务类型下会把
                // LLM_BATCH 子任务错标成 ECHO_BATCH,worker 路由到错处理器。
                String type = SubtaskDispatcher.typeOf(taskMapper.selectById(s.getTaskId()).getType(), s);
                kafkaTemplate.send(Topics.SUBTASK, String.valueOf(s.getId()),
                        new SubtaskMessage(s.getTaskId(), s.getId(), s.getSubtaskUuid(),
                                type, s.getSeq(), s.getInput()));
                traceEmitter.emit(String.valueOf(s.getTaskId()), s.getTaskId(), s.getId(),
                        TraceStage.TIMEOUT_REDISPATCH, "DISPATCHED",
                        "redispatch=" + (s.getRedispatchCount() + 1));
                log.warn("超时重投 subtaskId={} redispatch={}", s.getId(), s.getRedispatchCount() + 1);
            } else {
                if (stateMachine.transitionSubtask(s.getId(),
                        SubtaskStatus.DISPATCHED, SubtaskStatus.FAILED)) {
                    s.setStatus(SubtaskStatus.FAILED.name());
                    s.setErrorMsg("超时未回传,重投耗尽");
                    subtaskMapper.updateById(s);
                    taskMapper.incrementFailed(s.getTaskId());
                    stageAdvancer.onSettled(s.getTaskId());
                    log.error("超时失败落定 subtaskId={}", s.getId());
                }
            }
        }
    }
}
