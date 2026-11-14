package com.agentflow.server.service;

import com.agentflow.common.mq.ResultMessage;
import com.agentflow.server.entity.CourseEntity;
import com.agentflow.server.entity.LectureEntity;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.CourseMapper;
import com.agentflow.server.mapper.LectureMapper;
import com.agentflow.server.mapper.SubtaskMapper;
import com.agentflow.server.mapper.TaskMapper;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** LECTURE_INGEST 的五步推进,经 ResultHandleService 真实入口,最后讲次那一行被写对。 */
@SpringBootTest
@Transactional
class LectureIngestFlowTest {

    private static final String KEY = "0123456789abcdef0123456789abcdef";
    @Autowired private ResultHandleService service;
    @Autowired private TaskMapper taskMapper;
    @Autowired private SubtaskMapper subtaskMapper;
    @Autowired private CourseMapper courseMapper;
    @Autowired private LectureMapper lectureMapper;
    @MockBean private KafkaTemplate<String, Object> kafkaTemplate;

    private long lectureId;
    private long taskId;

    /** 一门课、一讲(INGESTING)、一个 RUNNING 的任务和已发出的 LEC_IMPORT;返回 LEC_IMPORT 的子任务 id。 */
    private long fixture() {
        CourseEntity c = new CourseEntity();
        c.setName("flow-test");
        courseMapper.insert(c);
        LectureEntity l = new LectureEntity();
        l.setCourseId(c.getId());
        l.setSeq(1);
        l.setSourceUrl("https://youtu.be/abc");
        l.setStatus("INGESTING");
        lectureMapper.insert(l);
        lectureId = l.getId();
        TaskEntity t = new TaskEntity();
        t.setTaskUuid(UUID.randomUUID().toString());
        t.setType("LECTURE_INGEST");
        t.setStatus("RUNNING");
        t.setPayload("{\"lectureId\":" + lectureId + ",\"courseId\":" + c.getId() + "}");
        t.setSubtaskTotal(1);
        t.setSubtaskDone(0);
        t.setSubtaskFailed(0);
        taskMapper.insert(t);
        taskId = t.getId();
        SubtaskEntity s = new SubtaskEntity();
        s.setSubtaskUuid(UUID.randomUUID().toString());
        s.setTaskId(taskId);
        s.setSeq(0);
        s.setKind("LEC_IMPORT");
        s.setStatus("DISPATCHED");
        s.setInput("{\"lectureId\":" + lectureId + ",\"url\":\"https://youtu.be/abc\"}");
        subtaskMapper.insert(s);
        return s.getId();
    }

    /** 这个 kind 的子任务(刚由阶段推进建出来,PENDING),标成已发出后返回它的 id。 */
    private long dispatched(String kind) {
        SubtaskEntity s = subtaskMapper.selectOne(new QueryWrapper<SubtaskEntity>().eq("task_id", taskId).eq("kind", kind));
        assertEquals("PENDING", s.getStatus(), kind);
        subtaskMapper.casStatus(s.getId(), "PENDING", "DISPATCHED");
        return s.getId();
    }

    private void ok(long subtaskId, String output) {
        service.handle(new ResultMessage(taskId, subtaskId, true, output, null));
    }

    @Test
    void fiveStepsThenTheLectureIsReady() {
        ok(fixture(), "{\"mediaKey\":\"" + KEY + "\",\"title\":\"LLM Agents Overview\",\"durationMs\":7101000,\"hasVideo\":true}");
        long transcribe = dispatched("LEC_TRANSCRIBE");
        long frames = dispatched("LEC_FRAMES");
        ok(transcribe, "{\"utterances\":1298,\"cached\":false}");
        assertTrue(subtaskMapper.selectList(new QueryWrapper<SubtaskEntity>().eq("task_id", taskId).eq("kind", "LEC_INDEX")).isEmpty(),
                "截帧还没落定,不能开始建索引");
        ok(frames, "{\"frames\":120,\"cached\":false}");
        ok(dispatched("LEC_INDEX"), "{\"speech\":150,\"slides\":120}");
        assertEquals("INGESTING", lectureMapper.selectById(lectureId).getStatus());
        ok(dispatched("LEC_CONCEPTS"), "{\"concepts\":[{\"name\":\"Pre-training\",\"startMs\":0,\"summary\":\"s\"}],\"usage\":{}}");

        assertEquals("COMPLETED", taskMapper.selectById(taskId).getStatus());
        LectureEntity l = lectureMapper.selectById(lectureId);
        assertEquals("READY", l.getStatus());
        assertEquals(KEY, l.getMediaKey());
        assertEquals("LLM Agents Overview", l.getTitle());
        assertTrue(l.getConcepts().contains("Pre-training"));
    }

    @Test
    void aFailedTranscriptionFailsTheLectureAndSaysWhy() {
        ok(fixture(), "{\"mediaKey\":\"" + KEY + "\",\"title\":\"T\",\"durationMs\":1000,\"hasVideo\":true}");
        long transcribe = dispatched("LEC_TRANSCRIBE");
        long frames = dispatched("LEC_FRAMES");
        service.handle(new ResultMessage(taskId, transcribe, false, null, "CUDA out of memory"));
        ok(frames, "{\"frames\":3,\"cached\":false}");

        assertEquals("FAILED", taskMapper.selectById(taskId).getStatus());
        LectureEntity l = lectureMapper.selectById(lectureId);
        assertEquals("FAILED", l.getStatus());
        assertEquals("LEC_TRANSCRIBE: CUDA out of memory", l.getError());
    }

    @Test
    void aSettledStepRecordsWhenItSettled() {
        long importId = fixture();
        // 用库里的时间做基准,不用 JVM 的时钟:两边时区可能不一样
        java.time.LocalDateTime dispatchedAt = subtaskMapper.selectById(importId).getUpdatedAt().minusMinutes(5);
        subtaskMapper.setUpdatedAt(importId, dispatchedAt);          // 这一步 5 分钟前发出去
        ok(importId, "{\"mediaKey\":\"" + KEY + "\",\"title\":\"T\",\"durationMs\":1000,\"hasVideo\":true}");
        java.time.LocalDateTime settledAt = subtaskMapper.selectById(importId).getUpdatedAt();
        assertTrue(settledAt.isAfter(dispatchedAt.plusMinutes(4)),
                "落定后 updated_at 应该是落定的时间(页面靠它算每一步的用时),实际还是发出时的 " + settledAt);
    }
}
