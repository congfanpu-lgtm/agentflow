package com.agentflow.server.coordinator;

import org.apache.kafka.common.PartitionInfo;

import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.common.mq.Topics;
import com.agentflow.common.state.SubtaskStatus;
import com.agentflow.common.state.TaskStatus;
import com.agentflow.common.trace.TraceStage;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.service.TaskStateMachine;
import com.agentflow.server.trace.TraceEmitter;
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
    private final TraceEmitter traceEmitter;

    public void dispatch(TaskEntity task) {
        if (!stateMachine.transitionTask(task.getId(), TaskStatus.PENDING, TaskStatus.RUNNING)) {
            log.warn("任务已被并发分发,跳过 taskId={}", task.getId());
            return;
        }
        dispatchPending(task);
    }

    /** 发送该任务所有 PENDING 子任务(提交时 / 阶段推进后共用;不做 task 状态迁移)。 */
    public void dispatchPending(TaskEntity task) {
        List<SubtaskEntity> subs = subtaskMapper.selectList(
                new QueryWrapper<SubtaskEntity>()
                        .eq("task_id", task.getId())
                        .eq("status", SubtaskStatus.PENDING.name()));
        // 显式分区:同一阶段的子任务 id 连续,按 id % 分区数各落一个分区 → 被不同消费线程并行处理。
        // 按 key 哈希时 3 个 agent 常撞进同一分区,max-poll-records=1 下只能排队串行。
        List<PartitionInfo> parts = kafkaTemplate.partitionsFor(Topics.SUBTASK);
        int partitions = parts == null ? 0 : parts.size();
        for (SubtaskEntity sub : subs) {
            SubtaskMessage msg = new SubtaskMessage(
                    task.getId(), sub.getId(), sub.getSubtaskUuid(),
                    typeOf(task.getType(), sub), sub.getSeq(), sub.getInput());
            try {
                // .get() 同步等待 broker ack:发送成功才迁移状态
                String key = String.valueOf(sub.getId());
                (partitions > 0
                        ? kafkaTemplate.send(Topics.SUBTASK, (int) (sub.getId() % partitions), key, msg)
                        : kafkaTemplate.send(Topics.SUBTASK, key, msg)).get();
                stateMachine.transitionSubtask(sub.getId(),
                        SubtaskStatus.PENDING, SubtaskStatus.DISPATCHED);
                traceEmitter.emit(String.valueOf(task.getId()), task.getId(), sub.getId(),
                        TraceStage.DISPATCHED, "DISPATCHED", "seq=" + sub.getSeq());
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();  // 仅在真被中断时恢复中断标志
                }
                log.error("子任务消息发送失败 subtaskId={},保持 PENDING 等待兜底",
                        sub.getId(), e);
            }
        }
    }

    /** worker 按此路由处理器:子任务有自己的 kind 就用 kind,否则沿用任务类型。 */
    public static String typeOf(String taskType, SubtaskEntity sub) {
        return sub.getKind() != null ? sub.getKind() : taskType;
    }
}
