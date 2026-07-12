package com.agentflow.server.service;

import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.common.mq.Topics;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

/**
 * DLQ 恢复:把已 FAILED 的子任务重置为 DISPATCHED(消息重投即视为"已在途"),回退任务失败计数并复活为
 * RUNNING,再重投主 topic。明确"谁来恢复":运营者按 subtaskId 显式触发。
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
        // 1. 子任务 FAILED → DISPATCHED(而非 PENDING!),显式清 error_msg、重投计数归零(UpdateWrapper 保证
        //    null 也写入;MyBatis-Plus 默认 updateById 的 NOT_NULL 策略会静默跳过 null 字段,清不掉旧的错误信息)
        //    P1 修复(2026-07-12 实盘故障演练复现):必须落地为 DISPATCHED,因为本方法在同一次调用里已经把消息
        //    重投到主 topic(见下方步骤 3)——消息正在"飞行中"、等待 worker 回传结果。
        //    ResultHandleService.handle() 的 transitionSubtask(DISPATCHED → COMPLETED/FAILED) 是一个 CAS,
        //    要求当前 DB 状态必须精确是 DISPATCHED 才会推进;之前误置为 PENDING 导致这个 CAS 恒不匹配,
        //    worker 处理完回传的结果被当成"子任务状态已变更,忽略重复结果"静默丢弃,计数永远不再推进,
        //    任务永久卡在 RUNNING。这里的语义与 SubtaskDispatcher 的正常分发路径完全一致——
        //    SubtaskDispatcher 也是"消息发出 = 子任务进入 DISPATCHED",发送与状态迁移在同一动作里完成;
        //    send 与状态迁移的顺序不追求严格原子(即便 send 提前于事务提交或反之),
        //    也有 worker 端处理耗时天然起到缓冲作用,与既有 dispatcher 的容忍模式一致。
        subtaskMapper.update(null, new UpdateWrapper<SubtaskEntity>()
                .eq("id", s.getId())
                .set("status", "DISPATCHED")
                .set("error_msg", null)
                .set("redispatch_count", 0));
        // 2. 任务失败计数原子回退 + 复活为 RUNNING + 显式清 result
        //    UpdateWrapper 只 set(status, result),不碰 subtask_failed,与 decrementFailed 的原子递减互不影响,顺序无关。
        TaskEntity t = taskMapper.selectById(s.getTaskId());
        taskMapper.decrementFailed(t.getId());
        taskMapper.update(null, new UpdateWrapper<TaskEntity>()
                .eq("id", t.getId())
                .set("status", "RUNNING")
                .set("result", null));
        // 3. 重投主 topic(attempt 归零,不带 header)
        kafkaTemplate.send(Topics.SUBTASK, String.valueOf(s.getId()),
                new SubtaskMessage(t.getId(), s.getId(), s.getSubtaskUuid(),
                        t.getType(), s.getSeq(), s.getInput()));
        log.info("DLQ 恢复:子任务重置并重投 subtaskId={} taskId={}", subtaskId, t.getId());
    }
}
