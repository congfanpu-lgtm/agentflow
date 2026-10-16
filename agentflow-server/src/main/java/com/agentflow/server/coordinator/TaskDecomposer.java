package com.agentflow.server.coordinator;

import com.agentflow.common.state.TaskStatus;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.fasterxml.jackson.databind.JsonNode;

import java.util.List;

/** 协调器拆解策略:每种任务类型一个实现,Spring 自动注册(策略模式)。 */
public interface TaskDecomposer {
    String type();

    List<SubtaskDef> decompose(JsonNode payload);

    /**
     * 当前所有子任务都已落定时调用:返回下一阶段要创建的子任务;空 = 没有下一阶段,任务进入终态。
     * 单阶段任务类型不必覆盖。
     */
    default List<SubtaskDef> nextStage(TaskEntity task, List<SubtaskEntity> subs) {
        return List.of();
    }

    /** 全部落定、即将进终态时调用:可按业务语义改判计数得出的终态。默认不改。 */
    default TaskStatus terminalStatus(TaskEntity task, List<SubtaskEntity> subs, TaskStatus computed) {
        return computed;
    }
}
