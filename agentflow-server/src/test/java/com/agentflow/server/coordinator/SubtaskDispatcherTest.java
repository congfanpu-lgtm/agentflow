package com.agentflow.server.coordinator;

import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.common.mq.Topics;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import com.agentflow.server.service.TaskSubmitService;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.RecordBatch;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@SpringBootTest
@Transactional
class SubtaskDispatcherTest {

    @Autowired
    private TaskSubmitService submitService;
    @Autowired
    private TaskMapper taskMapper;
    @Autowired
    private SubtaskMapper subtaskMapper;
    @Autowired
    private ObjectMapper om;
    @MockBean
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Test
    void dispatchSendsAllSubtasksAndFlipsStates() throws Exception {
        // KafkaTemplate.send(topic, key, value) 返回一个已完成的 future(发送成功)
        var record = new ProducerRecord<String, Object>(Topics.SUBTASK, "k", "v");
        var meta = new RecordMetadata(new TopicPartition(Topics.SUBTASK, 0), 0L, 0, 0L, 0, 0);
        var done = CompletableFuture.completedFuture(new SendResult<>(record, meta));
        when(kafkaTemplate.send(eq(Topics.SUBTASK), anyString(), any())).thenReturn(done);

        // submit() 内部已含分发(persist 事务提交后 dispatch)
        var payload = om.readTree("{\"items\":[\"a\",\"b\"]}");
        TaskEntity task = submitService.submit("ECHO_BATCH", payload);

        // task 进入 RUNNING
        assertEquals("RUNNING", taskMapper.selectById(task.getId()).getStatus());
        // 每个 subtask 发了一条消息且状态 DISPATCHED
        verify(kafkaTemplate, times(2)).send(eq(Topics.SUBTASK), anyString(), any());
        List<SubtaskEntity> subs = subtaskMapper.selectList(
                new QueryWrapper<SubtaskEntity>().eq("task_id", task.getId()));
        assertTrue(subs.stream().allMatch(s -> s.getStatus().equals("DISPATCHED")));

        // 消息内容正确(key = subtaskId 字符串,value = SubtaskMessage)
        ArgumentCaptor<Object> valueCaptor = ArgumentCaptor.forClass(Object.class);
        verify(kafkaTemplate, atLeastOnce())
                .send(eq(Topics.SUBTASK), anyString(), valueCaptor.capture());
        SubtaskMessage msg = (SubtaskMessage) valueCaptor.getAllValues().get(0);
        assertEquals(task.getId(), msg.getTaskId());
        assertEquals("ECHO_BATCH", msg.getType());
        assertNotNull(msg.getInputJson());
    }
}
