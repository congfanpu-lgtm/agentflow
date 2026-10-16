package com.agentflow.server.coordinator;

import com.agentflow.common.state.SubtaskStatus;
import com.agentflow.server.entity.SubtaskEntity;

import java.util.UUID;

/** 拆解产物:一个待创建子任务的定义。kind 为空 → worker 按任务类型路由(旧任务类型)。 */
public record SubtaskDef(int seq, String inputJson, String kind) {

    public SubtaskDef(int seq, String inputJson) {
        this(seq, inputJson, null);
    }

    public SubtaskEntity toEntity(Long taskId) {
        SubtaskEntity sub = new SubtaskEntity();
        sub.setSubtaskUuid(UUID.randomUUID().toString());
        sub.setTaskId(taskId);
        sub.setSeq(seq);
        sub.setKind(kind);
        sub.setStatus(SubtaskStatus.PENDING.name());
        sub.setInput(inputJson);
        return sub;
    }
}
