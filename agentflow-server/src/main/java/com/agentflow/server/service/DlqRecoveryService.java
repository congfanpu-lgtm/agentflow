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
        // 2. 任务复活为 RUNNING + 失败计数回退
        // 注意顺序:先整体 updateById(全字段覆盖式写回,此时 subtaskFailed 与 DB 一致未变),
        // 再原子 decrementFailed——避免整体更新用内存中的旧值把原子递减结果覆盖回去。
        TaskEntity t = taskMapper.selectById(s.getTaskId());
        t.setStatus("RUNNING");
        t.setResult(null);
        taskMapper.updateById(t);
        taskMapper.decrementFailed(t.getId());
        // 3. 重投主 topic(attempt 归零,不带 header)
        kafkaTemplate.send(Topics.SUBTASK, String.valueOf(s.getId()),
                new SubtaskMessage(t.getId(), s.getId(), s.getSubtaskUuid(),
                        t.getType(), s.getSeq(), s.getInput()));
        log.info("DLQ 恢复:子任务重置并重投 subtaskId={} taskId={}", subtaskId, t.getId());
    }
}
