package com.agentflow.server.service;

import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.common.mq.Topics;
import com.agentflow.common.state.SubtaskStatus;
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

    @Value("${agentflow.timeout.stuck-seconds:60}")
    private long stuckSeconds;

    @Transactional
    public void sweep() {
        LocalDateTime cutoff = LocalDateTime.now().minusSeconds(stuckSeconds);
        List<SubtaskEntity> stuck = subtaskMapper.findStuckDispatched(cutoff);
        for (SubtaskEntity s : stuck) {
            if (s.getRedispatchCount() < MAX_REDISPATCH) {
                subtaskMapper.incrementRedispatch(s.getId());
                // 重投后 incrementRedispatch 的 ON UPDATE 会刷新 updated_at,重新计时
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
                    taskFinalizer.finalizeIfAllSettled(s.getTaskId());
                    log.error("超时失败落定 subtaskId={}", s.getId());
                }
            }
        }
    }
}
