package com.agentflow.server.coordinator;

import com.agentflow.common.trace.TraceStage;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import com.agentflow.server.service.TaskFinalizer;
import com.agentflow.server.trace.TraceEmitter;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * 子任务落定后的唯一入口(结果回传、超时兜底两条触发源共用):
 * 当前全部落定且任务类型有下一阶段 → 同一事务内建下一阶段子任务并调大 total,提交后再分发;
 * 否则交 {@link TaskFinalizer} 判终态。
 *
 * <p>并发:计数累加是对 task 行的 UPDATE,持行锁到事务提交,多个结果并发回来时串行化;
 * 只有最后一个看到 done+failed==total,所以下一阶段只会创建一次。
 */
@Slf4j
@Component
public class StageAdvancer {

    private final Map<String, TaskDecomposer> decomposers;
    private final TaskMapper taskMapper;
    private final SubtaskMapper subtaskMapper;
    private final SubtaskDispatcher dispatcher;
    private final TaskFinalizer taskFinalizer;
    private final TraceEmitter traceEmitter;
    private final ObjectMapper objectMapper;

    public StageAdvancer(List<TaskDecomposer> decomposerList, TaskMapper taskMapper, SubtaskMapper subtaskMapper,
                         SubtaskDispatcher dispatcher, TaskFinalizer taskFinalizer,
                         TraceEmitter traceEmitter, ObjectMapper objectMapper) {
        this.decomposers = decomposerList.stream()
                .collect(Collectors.toMap(TaskDecomposer::type, Function.identity()));
        this.taskMapper = taskMapper;
        this.subtaskMapper = subtaskMapper;
        this.dispatcher = dispatcher;
        this.taskFinalizer = taskFinalizer;
        this.traceEmitter = traceEmitter;
        this.objectMapper = objectMapper;
    }

    public void onSettled(Long taskId) {
        TaskEntity task = taskMapper.selectById(taskId);
        if (task.getSubtaskDone() + task.getSubtaskFailed() == task.getSubtaskTotal()) {
            TaskDecomposer decomposer = decomposers.get(task.getType());
            if (decomposer != null) {
                List<SubtaskEntity> subs = subtaskMapper.selectList(new QueryWrapper<SubtaskEntity>()
                        .eq("task_id", taskId).orderByAsc("seq"));
                List<SubtaskDef> next = decomposer.nextStage(task, subs);
                if (!next.isEmpty()) {
                    advance(task, next);
                    return;
                }
            }
        }
        taskFinalizer.finalizeIfAllSettled(taskId);
    }

    private void advance(TaskEntity task, List<SubtaskDef> next) {
        ArrayNode created = objectMapper.createArrayNode();
        for (SubtaskDef def : next) {
            SubtaskEntity sub = def.toEntity(task.getId());
            subtaskMapper.insert(sub);
            ObjectNode item = created.addObject().put("subtaskId", sub.getId()).put("seq", def.seq());
            try {
                item.set("input", objectMapper.readTree(def.inputJson()));
            } catch (Exception e) {
                item.put("input", def.inputJson());
            }
        }
        // 与本次结果落定同一事务:total 先变大,TaskFinalizer 不会把任务提前判终态
        taskMapper.addTotal(task.getId(), next.size());
        String kind = next.get(0).kind();
        ObjectNode detail = objectMapper.createObjectNode().put("kind", kind);
        detail.set("subtasks", created);
        traceEmitter.emit(String.valueOf(task.getId()), task.getId(), null,
                TraceStage.STAGE_ADVANCED, kind, detail.toString());
        log.info("任务进入下一阶段 taskId={} kind={} count={}", task.getId(), kind, next.size());

        // 事务提交后才发 MQ:否则 worker 可能先于提交收到消息,回传结果时查不到子任务
        Runnable dispatch = () -> dispatcher.dispatchPending(task);
        if (TransactionSynchronizationManager.isSynchronizationActive()) {
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCommit() {
                    dispatch.run();
                }
            });
        } else {
            dispatch.run();
        }
    }
}
