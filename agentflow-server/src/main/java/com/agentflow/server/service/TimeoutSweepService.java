package com.agentflow.server.service;

import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.common.mq.Topics;
import com.agentflow.common.state.SubtaskStatus;
import com.agentflow.common.trace.TraceStage;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import com.agentflow.server.trace.TraceEmitter;
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
 * <p>
 * 与「处理异常重试」(worker 侧 RetryRouter 的重试阶梯,约 5s+30s+5m ≈ 335s)按【触发源】区分,
 * 二者不是同一回事:重试阶梯处理的是"处理异常"(worker 收到消息但处理失败,topic 路由到延迟重试
 * topic,全程不落库、不更新 updated_at);本服务处理的是"静默失败"(worker 从未处理——进程死亡
 * 或消息丢失,既无结果回传也无重试活动)。{@code stuck-seconds} 默认值刻意设置在整条重试阶梯
 * 总时长之上(留有余量),使兜底只对"确实静默"的子任务生效,不会在阶梯进行中把它误判为卡死、
 * 抢先重投/落定,从而打断正在进行的重试。
 * 终态落定复用 {@link TaskFinalizer},与 {@link ResultHandleService} 共用同一套判终态/聚合逻辑(DRY)。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class TimeoutSweepService {

    static final int MAX_REDISPATCH = 3;

    private final SubtaskMapper subtaskMapper;
    private final TaskMapper taskMapper;
    private final TaskStateMachine stateMachine;
    private final TaskFinalizer taskFinalizer;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final TraceEmitter traceEmitter;

    // 默认 600s:高于重试阶梯全程耗时(约 5s+30s+5m ≈ 335s)且留有余量,避免抢占仍在阶梯中的子任务。
    @Value("${agentflow.timeout.stuck-seconds:600}")
    private long stuckSeconds;

    @Transactional
    public void sweep() {
        LocalDateTime cutoff = LocalDateTime.now().minusSeconds(stuckSeconds);
        List<SubtaskEntity> stuck = subtaskMapper.findStuckDispatched(cutoff);
        for (SubtaskEntity s : stuck) {
            if (s.getRedispatchCount() < MAX_REDISPATCH) {
                subtaskMapper.incrementRedispatch(s.getId());
                // 重投后 incrementRedispatch 的 ON UPDATE 会刷新 updated_at,重新计时。
                // 修 backlog T4b:type 取自任务而非硬编码 ECHO_BATCH,否则多任务类型下会把
                // LLM_BATCH 子任务错标成 ECHO_BATCH,worker 路由到错处理器。
                String type = taskMapper.selectById(s.getTaskId()).getType();
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
                    taskFinalizer.finalizeIfAllSettled(s.getTaskId());
                    log.error("超时失败落定 subtaskId={}", s.getId());
                }
            }
        }
    }
}
