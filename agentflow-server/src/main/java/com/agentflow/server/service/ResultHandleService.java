package com.agentflow.server.service;

import com.agentflow.common.mq.ResultMessage;
import com.agentflow.common.state.SubtaskStatus;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * 结果回收(部分失败语义):子任务状态落定 → 按成败原子累加计数 →
 * done+failed==total 时判终态:全成 COMPLETED / 部分成 PARTIAL_FAILED / 全败 FAILED。
 * 终态判定用「计数列 + CAS」而非 COUNT(*),并发下只有一个回调触发聚合。
 * 终态判定 + 聚合逻辑抽到 {@link TaskFinalizer},与超时兜底 {@link TimeoutSweepService} 共用。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ResultHandleService {

    private final TaskMapper taskMapper;
    private final SubtaskMapper subtaskMapper;
    private final TaskStateMachine stateMachine;
    private final TaskFinalizer taskFinalizer;

    @Transactional
    public void handle(ResultMessage msg) {
        SubtaskEntity sub = subtaskMapper.selectById(msg.getSubtaskId());
        if (sub == null) {
            log.error("结果对应的子任务不存在 subtaskId={}", msg.getSubtaskId());
            return;
        }
        SubtaskStatus target = msg.isSuccess() ? SubtaskStatus.COMPLETED : SubtaskStatus.FAILED;
        if (!stateMachine.transitionSubtask(sub.getId(), SubtaskStatus.DISPATCHED, target)) {
            log.warn("子任务状态已变更,忽略重复结果 subtaskId={}", sub.getId());
            return;
        }
        sub.setStatus(target.name());
        if (msg.isSuccess()) {
            sub.setOutput(msg.getOutputJson());
        } else {
            sub.setErrorMsg(msg.getErrorMsg());
        }
        subtaskMapper.updateById(sub);

        if (msg.isSuccess()) {
            taskMapper.incrementDone(msg.getTaskId());
        } else {
            taskMapper.incrementFailed(msg.getTaskId());
        }
        taskFinalizer.finalizeIfAllSettled(msg.getTaskId());
    }
}
