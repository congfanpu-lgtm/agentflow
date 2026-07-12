package com.agentflow.server.service;

import com.agentflow.common.state.TaskStatus;
import com.agentflow.common.trace.TraceStage;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import com.agentflow.server.trace.TraceEmitter;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 任务终态落定的唯一入口:两条独立触发源(结果回传 {@link ResultHandleService}、
 * 超时兜底 {@link TimeoutSweepService})共用同一套「判终态 + 聚合结果」逻辑,避免重复实现
 * 走样导致两处语义漂移。
 * 全部子任务落定(done+failed==total)才判终态;CAS 保证并发下只聚合一次。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class TaskFinalizer {

    private final TaskMapper taskMapper;
    private final SubtaskMapper subtaskMapper;
    private final TaskStateMachine stateMachine;
    private final ObjectMapper objectMapper;
    private final TraceEmitter traceEmitter;
    private final NotificationService notificationService;

    public void finalizeIfAllSettled(Long taskId) {
        TaskEntity task = taskMapper.selectById(taskId);
        if (task.getSubtaskDone() + task.getSubtaskFailed() < task.getSubtaskTotal()) {
            return;  // 还有子任务在跑,不能提前进终态
        }
        TaskStatus terminal;
        if (task.getSubtaskFailed() == 0) {
            terminal = TaskStatus.COMPLETED;
        } else if (task.getSubtaskDone() == 0) {
            terminal = TaskStatus.FAILED;
        } else {
            terminal = TaskStatus.PARTIAL_FAILED;  // 部分失败:成功输出保留,失败原因可见
        }
        if (stateMachine.transitionTask(taskId, TaskStatus.RUNNING, terminal)) {
            aggregate(task, terminal);
        }
    }

    private void aggregate(TaskEntity task, TaskStatus terminal) {
        List<SubtaskEntity> subs = subtaskMapper.selectList(
                new QueryWrapper<SubtaskEntity>()
                        .eq("task_id", task.getId()).orderByAsc("seq"));
        ArrayNode arr = objectMapper.createArrayNode();
        for (SubtaskEntity s : subs) {
            ObjectNode item = objectMapper.createObjectNode();
            item.put("seq", s.getSeq());
            item.put("status", s.getStatus());
            try {
                item.set("output", s.getOutput() == null
                        ? null : objectMapper.readTree(s.getOutput()));
            } catch (Exception e) {
                item.put("output", s.getOutput());
            }
            if (s.getErrorMsg() != null) {
                item.put("error", s.getErrorMsg());
            }
            arr.add(item);
        }
        task.setResult(arr.toString());
        // 实体是 CAS 前查出来的,status 字段还是 RUNNING;
        // 必须同步为终态,否则 updateById 会把旧状态写回去,冲掉 CAS 结果
        task.setStatus(terminal.name());
        taskMapper.updateById(task);
        traceEmitter.emit(String.valueOf(task.getId()), task.getId(), null,
                TraceStage.TASK_FINALIZED, terminal.name(), "subtasks=" + subs.size());
        notificationService.notifyTaskFinished(task);
        log.info("任务落定 taskId={} terminal={} subtasks={}", task.getId(), terminal, subs.size());
    }
}
