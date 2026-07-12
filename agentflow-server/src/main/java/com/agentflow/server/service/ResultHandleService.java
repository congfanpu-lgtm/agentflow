package com.agentflow.server.service;

import com.agentflow.common.mq.ResultMessage;
import com.agentflow.common.state.SubtaskStatus;
import com.agentflow.common.state.TaskStatus;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * 结果回收(部分失败语义):子任务状态落定 → 按成败原子累加计数 →
 * done+failed==total 时判终态:全成 COMPLETED / 部分成 PARTIAL_FAILED / 全败 FAILED。
 * 终态判定用「计数列 + CAS」而非 COUNT(*),并发下只有一个回调触发聚合。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class ResultHandleService {

    private final TaskMapper taskMapper;
    private final SubtaskMapper subtaskMapper;
    private final TaskStateMachine stateMachine;
    private final ObjectMapper objectMapper;

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
        finalizeIfAllSettled(msg.getTaskId());
    }

    /** 全部子任务落定(done+failed==total)才判终态;CAS 保证并发下只聚合一次。 */
    private void finalizeIfAllSettled(Long taskId) {
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
        log.info("任务落定 taskId={} terminal={} subtasks={}", task.getId(), terminal, subs.size());
    }
}
