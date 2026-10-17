package com.agentflow.server.service;

import com.agentflow.common.mq.ResultMessage;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;

/**
 * RESEARCH_AGENT 多阶段推进(plan → agents → reflect → 追问 agent → synthesize,经 ResultHandleService 真实入口)。
 * 测试事务最终回滚,afterCommit 的分发不会触发 → 新阶段子任务停在 PENDING,便于断言。
 */
@SpringBootTest
@Transactional
class ResearchAgentFlowTest {

    @Autowired private ResultHandleService service;
    @Autowired private TaskMapper taskMapper;
    @Autowired private SubtaskMapper subtaskMapper;
    @MockBean private KafkaTemplate<String, Object> kafkaTemplate;

    /** RUNNING 的 RESEARCH_AGENT 任务 + 1 个 DISPATCHED 的 PLAN 子任务,返回 [taskId, planId]。 */
    private long[] fixture() {
        TaskEntity t = new TaskEntity();
        t.setTaskUuid(UUID.randomUUID().toString());
        t.setType("RESEARCH_AGENT");
        t.setStatus("RUNNING");
        t.setPayload("{\"question\":\"kafka vs rabbitmq\",\"reflect\":true}");   // 走完整的反思路径
        t.setSubtaskTotal(1);
        t.setSubtaskDone(0);
        t.setSubtaskFailed(0);
        taskMapper.insert(t);
        SubtaskEntity p = new SubtaskEntity();
        p.setSubtaskUuid(UUID.randomUUID().toString());
        p.setTaskId(t.getId());
        p.setSeq(0);
        p.setKind("PLAN");
        p.setStatus("DISPATCHED");
        p.setInput("{\"question\":\"kafka vs rabbitmq\"}");
        subtaskMapper.insert(p);
        return new long[]{t.getId(), p.getId()};
    }

    private List<SubtaskEntity> subs(long taskId, String kind) {
        return subtaskMapper.selectList(new QueryWrapper<SubtaskEntity>()
                .eq("task_id", taskId).eq("kind", kind).orderByAsc("seq"));
    }

    private void dispatched(SubtaskEntity s) {
        subtaskMapper.casStatus(s.getId(), "PENDING", "DISPATCHED");
    }

    @Test
    void planAgentsReflectFollowUpSynthesizeThenPartialFailed() {
        long[] ids = fixture();
        long taskId = ids[0];

        service.handle(new ResultMessage(taskId, ids[1], true, "{\"subQuestions\":[\"a\",\"b\"]}", null));
        TaskEntity t = taskMapper.selectById(taskId);
        assertEquals("RUNNING", t.getStatus());
        assertEquals(3, t.getSubtaskTotal());
        List<SubtaskEntity> agents = subs(taskId, "AGENT");
        assertEquals(2, agents.size());
        assertEquals("PENDING", agents.get(0).getStatus());

        agents.forEach(this::dispatched);
        service.handle(new ResultMessage(taskId, agents.get(0).getId(), true,
                "{\"answer\":\"A1\",\"sources\":[]}", null));
        assertTrue(subs(taskId, "SYNTHESIZE").isEmpty());   // 还有 agent 没落定,不能合成

        service.handle(new ResultMessage(taskId, agents.get(1).getId(), false, null, "boom"));
        assertTrue(subs(taskId, "SYNTHESIZE").isEmpty());   // agent 落定后先反思,不直接合成
        List<SubtaskEntity> reflect = subs(taskId, "REFLECT");
        assertEquals(1, reflect.size());
        assertEquals(3, reflect.get(0).getSeq());
        assertEquals(4, taskMapper.selectById(taskId).getSubtaskTotal());

        dispatched(reflect.get(0));
        service.handle(new ResultMessage(taskId, reflect.get(0).getId(), true,
                "{\"gaps\":[\"c\"],\"reason\":\"r\"}", null));
        List<SubtaskEntity> followUps = subs(taskId, "AGENT").stream().filter(x -> x.getSeq() > 3).toList();
        assertEquals(1, followUps.size());                   // 反思发现 1 个缺口 → 1 个追问 agent
        assertEquals(4, followUps.get(0).getSeq());
        assertTrue(followUps.get(0).getInput().replace(" ", "").contains("\"followUp\":true"));
        assertEquals(5, taskMapper.selectById(taskId).getSubtaskTotal());

        dispatched(followUps.get(0));
        service.handle(new ResultMessage(taskId, followUps.get(0).getId(), true,
                "{\"answer\":\"C1\",\"sources\":[]}", null));
        List<SubtaskEntity> synth = subs(taskId, "SYNTHESIZE");
        assertEquals(1, synth.size());
        assertEquals(5, synth.get(0).getSeq());
        assertTrue(synth.get(0).getInput().contains("C1"), "追问结论要进入合成");
        assertEquals(6, taskMapper.selectById(taskId).getSubtaskTotal());
        assertEquals("RUNNING", taskMapper.selectById(taskId).getStatus());

        dispatched(synth.get(0));
        service.handle(new ResultMessage(taskId, synth.get(0).getId(), true,
                "{\"report\":\"R [1]\",\"sources\":[]}", null));
        t = taskMapper.selectById(taskId);
        assertEquals("PARTIAL_FAILED", t.getStatus());   // 一个 agent 失败:部分失败可见,报告仍在结果里
        assertTrue(t.getResult().contains("\"report\""));
        assertTrue(t.getResult().replace(" ", "").contains("\"kind\":\"SYNTHESIZE\""));   // MySQL JSON 列取回带空格
    }

    @Test
    void failedPlanEndsTaskFailedWithoutAgents() {
        long[] ids = fixture();
        service.handle(new ResultMessage(ids[0], ids[1], false, null, "LLM down"));
        assertEquals("FAILED", taskMapper.selectById(ids[0]).getStatus());
        assertTrue(subs(ids[0], "AGENT").isEmpty());
    }

    @Test
    void allAgentsFailedEndsFailedWithoutSynthesize() {
        long[] ids = fixture();
        long taskId = ids[0];
        service.handle(new ResultMessage(taskId, ids[1], true, "{\"subQuestions\":[\"a\",\"b\"]}", null));
        List<SubtaskEntity> agents = subs(taskId, "AGENT");
        agents.forEach(this::dispatched);
        service.handle(new ResultMessage(taskId, agents.get(0).getId(), false, null, "e1"));
        service.handle(new ResultMessage(taskId, agents.get(1).getId(), false, null, "e2"));

        assertTrue(subs(taskId, "SYNTHESIZE").isEmpty());
        assertEquals("FAILED", taskMapper.selectById(taskId).getStatus());   // 没有报告就是失败(spec §6)
    }
}
