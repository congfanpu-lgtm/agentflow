package com.agentflow.server.service;

import com.agentflow.common.state.SubtaskStatus;
import com.agentflow.common.state.TaskStatus;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

/**
 * 状态机唯一入口:枚举规则表校验合法性,数据库 CAS 保证并发安全。
 * 返回 false = CAS 竞争失败(状态已被别人改走),调用方自行决定重查或放弃。
 */
@Service
@RequiredArgsConstructor
public class TaskStateMachine {

    private final TaskMapper taskMapper;
    private final SubtaskMapper subtaskMapper;

    public boolean transitionTask(Long taskId, TaskStatus from, TaskStatus to) {
        if (!from.canTransitionTo(to)) {
            throw new IllegalStateException(
                    "非法任务状态迁移: " + from + " -> " + to + ", taskId=" + taskId);
        }
        return taskMapper.casStatus(taskId, from.name(), to.name()) == 1;
    }

    public boolean transitionSubtask(Long subtaskId, SubtaskStatus from, SubtaskStatus to) {
        if (!from.canTransitionTo(to)) {
            throw new IllegalStateException(
                    "非法子任务状态迁移: " + from + " -> " + to + ", subtaskId=" + subtaskId);
        }
        return subtaskMapper.casStatus(subtaskId, from.name(), to.name()) == 1;
    }
}
