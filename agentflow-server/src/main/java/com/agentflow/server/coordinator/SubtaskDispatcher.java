package com.agentflow.server.coordinator;

import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.common.mq.Topics;
import com.agentflow.common.state.SubtaskStatus;
import com.agentflow.common.state.TaskStatus;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.service.TaskStateMachine;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 分发器:task PENDING→RUNNING 后,把每个 PENDING 子任务同步发往 SUBTASK topic,
 * 发送成功才 PENDING→DISPATCHED。发送失败的子任务保持 PENDING(W3-4 由重试兜底)。
 * 消息 key = subtaskId,子任务相互独立,靠 key 均匀散列到分区、最大化并行消费。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SubtaskDispatcher {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final SubtaskMapper subtaskMapper;
    private final TaskStateMachine stateMachine;

    public void dispatch(TaskEntity task) {
        if (!stateMachine.transitionTask(task.getId(), TaskStatus.PENDING, TaskStatus.RUNNING)) {
            log.warn("任务已被并发分发,跳过 taskId={}", task.getId());
            return;
        }
        List<SubtaskEntity> subs = subtaskMapper.selectList(
                new QueryWrapper<SubtaskEntity>()
                        .eq("task_id", task.getId())
                        .eq("status", SubtaskStatus.PENDING.name()));
        for (SubtaskEntity sub : subs) {
            SubtaskMessage msg = new SubtaskMessage(
                    task.getId(), sub.getId(), sub.getSubtaskUuid(),
                    task.getType(), sub.getSeq(), sub.getInput());
            try {
                // .get() 同步等待 broker ack:发送成功才迁移状态
                kafkaTemplate.send(Topics.SUBTASK, String.valueOf(sub.getId()), msg).get();
                stateMachine.transitionSubtask(sub.getId(),
                        SubtaskStatus.PENDING, SubtaskStatus.DISPATCHED);
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();  // 仅在真被中断时恢复中断标志
                }
                log.error("子任务消息发送失败 subtaskId={},保持 PENDING 等待兜底",
                        sub.getId(), e);
            }
        }
    }
}
