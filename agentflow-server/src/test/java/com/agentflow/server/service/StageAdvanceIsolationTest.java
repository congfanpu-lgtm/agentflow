package com.agentflow.server.service;

import com.agentflow.common.mq.ResultMessage;
import com.agentflow.common.trace.TraceStage;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import com.agentflow.server.trace.TraceEmitter;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.doAnswer;

/**
 * 竞态复现:agent B 的失败结果正在处理(事务已开始读库),此时 agent A 的成功结果在另一个事务里提交。
 * B 累加计数后看到"全部落定",推进阶段时必须读到 A 已 COMPLETED——
 * 在 MySQL 默认的 REPEATABLE READ 下会读到事务开始时的旧快照(A 还是 DISPATCHED),下一阶段被漏掉。
 * 本测试不加 @Transactional:handle 必须是最外层事务,隔离级别注解才生效;数据在 @AfterEach 清理。
 */
@SpringBootTest
class StageAdvanceIsolationTest {

    @Autowired private ResultHandleService service;
    @Autowired private TaskMapper taskMapper;
    @Autowired private SubtaskMapper subtaskMapper;
    @Autowired private PlatformTransactionManager txManager;
    @MockBean private KafkaTemplate<String, Object> kafkaTemplate;
    @MockBean private TraceEmitter traceEmitter;

    private Long taskId;

    private SubtaskEntity sub(int seq, String kind, String status, String input) {
        SubtaskEntity s = new SubtaskEntity();
        s.setSubtaskUuid(UUID.randomUUID().toString());
        s.setTaskId(taskId);
        s.setSeq(seq);
        s.setKind(kind);
        s.setStatus(status);
        s.setInput(input);
        subtaskMapper.insert(s);
        return s;
    }

    @AfterEach
    void cleanup() {
        subtaskMapper.delete(new QueryWrapper<SubtaskEntity>().eq("task_id", taskId));
        taskMapper.deleteById(taskId);
    }

    @Test
    void stageAdvanceSeesResultCommittedByConcurrentTransaction() {
        TaskEntity t = new TaskEntity();
        t.setTaskUuid(UUID.randomUUID().toString());
        t.setType("RESEARCH_AGENT");
        t.setStatus("RUNNING");
        t.setPayload("{\"question\":\"q\"}");
        t.setSubtaskTotal(3);
        t.setSubtaskDone(1);   // PLAN 已完成
        t.setSubtaskFailed(0);
        taskMapper.insert(t);
        taskId = t.getId();
        SubtaskEntity plan = sub(0, "PLAN", "COMPLETED", "{\"question\":\"q\"}");
        plan.setOutput("{\"subQuestions\":[\"a\",\"b\"]}");
        subtaskMapper.updateById(plan);
        SubtaskEntity a = sub(1, "AGENT", "DISPATCHED", "{\"question\":\"a\"}");
        SubtaskEntity b = sub(2, "AGENT", "DISPATCHED", "{\"question\":\"b\"}");

        // B 的处理事务读过库之后(发 SUBTASK_SETTLED 轨迹时),A 的结果在独立事务里提交
        TransactionTemplate other = new TransactionTemplate(txManager);
        other.setPropagationBehavior(TransactionDefinition.PROPAGATION_REQUIRES_NEW);
        doAnswer(inv -> {
            other.executeWithoutResult(st -> {
                subtaskMapper.casStatus(a.getId(), "DISPATCHED", "COMPLETED");
                SubtaskEntity done = subtaskMapper.selectById(a.getId());
                done.setOutput("{\"answer\":\"A1\",\"sources\":[]}");
                subtaskMapper.updateById(done);
                taskMapper.incrementDone(taskId);
            });
            return null;
        }).when(traceEmitter).emit(anyString(), anyLong(), eq(b.getId()), eq(TraceStage.SUBTASK_SETTLED), any(), any());

        service.handle(new ResultMessage(taskId, b.getId(), false, null, "boom"));

        assertEquals(1, subtaskMapper.selectCount(new QueryWrapper<SubtaskEntity>()
                .eq("task_id", taskId).eq("kind", "REFLECT")), "A 已成功,必须进入下一阶段(反思)");
        assertEquals("RUNNING", taskMapper.selectById(taskId).getStatus());
    }
}
