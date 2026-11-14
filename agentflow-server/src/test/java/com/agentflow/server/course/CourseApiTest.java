package com.agentflow.server.course;

import com.agentflow.server.entity.LectureEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.LectureMapper;
import com.agentflow.server.mapper.TaskMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/** 课程接口:建课、加讲次(提交摄入任务)、看进度、重试、读逐字稿和帧。 */
@SpringBootTest(properties = "agentflow.course.enabled=true")
@AutoConfigureMockMvc
@Transactional
class CourseApiTest {

    private static final ObjectMapper OM = new ObjectMapper();
    private static final String KEY = "0123456789abcdef0123456789abcdef";
    @Autowired private MockMvc mvc;
    @Autowired private LectureMapper lectureMapper;
    @Autowired private TaskMapper taskMapper;
    @MockBean private KafkaTemplate<String, Object> kafkaTemplate;
    @MockBean private MediaProxy mediaProxy;

    private JsonNode postJson(String url, String body, int expected) throws Exception {
        String s = mvc.perform(post(url).contentType(MediaType.APPLICATION_JSON).content(body))
                .andExpect(status().is(expected)).andReturn().getResponse().getContentAsString(StandardCharsets.UTF_8);
        return OM.readTree(s.isEmpty() ? "{}" : s);
    }

    private long course() throws Exception {
        return postJson("/api/v1/courses", "{\"name\":\"Agentic AI\"}", 200).get("id").asLong();
    }

    @Test
    void addingALectureStartsItsIngest() throws Exception {
        long c = course();
        JsonNode l = postJson("/api/v1/courses/" + c + "/lectures", "{\"url\":\"https://youtu.be/abc\"}", 200);
        assertEquals("INGESTING", l.get("status").asText());
        assertEquals(1, l.get("seq").asInt());
        JsonNode second = postJson("/api/v1/courses/" + c + "/lectures", "{\"path\":\"lec2.mp4\",\"title\":\"第 2 讲\"}", 200);
        assertEquals(2, second.get("seq").asInt());

        mvc.perform(get("/api/v1/courses/" + c)).andExpect(status().isOk())
                .andExpect(jsonPath("$.name").value("Agentic AI"))
                .andExpect(jsonPath("$.lectures.length()").value(2))
                .andExpect(jsonPath("$.lectures[0].sourceUrl").value("https://youtu.be/abc"))
                .andExpect(jsonPath("$.lectures[0].stages[0].kind").value("LEC_IMPORT"))
                .andExpect(jsonPath("$.lectures[0].stages.length()").value(1))
                .andExpect(jsonPath("$.lectures[1].title").value("第 2 讲"));
        mvc.perform(get("/api/v1/courses")).andExpect(status().isOk())
                .andExpect(jsonPath("$[?(@.id == " + c + ")].lectures").value(2))
                .andExpect(jsonPath("$[?(@.id == " + c + ")].ready").value(0));
    }

    @Test
    void aLectureNeedsExactlyOneSourceAndAnExistingCourse() throws Exception {
        long c = course();
        postJson("/api/v1/courses/" + c + "/lectures", "{}", 400);
        postJson("/api/v1/courses/" + c + "/lectures", "{\"url\":\"https://youtu.be/abc\",\"path\":\"a.mp4\"}", 400);
        postJson("/api/v1/courses/999999999/lectures", "{\"url\":\"https://youtu.be/abc\"}", 404);
        postJson("/api/v1/courses", "{\"name\":\"  \"}", 400);
        mvc.perform(get("/api/v1/courses/999999999")).andExpect(status().isNotFound());
    }

    private long lecture(long course, String status, String mediaKey) {
        LectureEntity l = new LectureEntity();
        l.setCourseId(course);
        l.setSeq(1);
        l.setSourceUrl("https://youtu.be/abc");
        l.setStatus(status);
        l.setMediaKey(mediaKey);
        l.setError("FAILED".equals(status) ? "LEC_IMPORT: media 502: download failed" : null);
        l.setConcepts("READY".equals(status) ? "[{\"name\":\"Pre-training\",\"startMs\":0,\"summary\":\"s\"}]" : null);
        lectureMapper.insert(l);
        return l.getId();
    }

    @Test
    void retryIsOnlyForFailedOrPartialLectures() throws Exception {
        long c = course();
        long failed = lecture(c, "FAILED", null);
        JsonNode again = postJson("/api/v1/lectures/" + failed + "/retry", "", 200);
        assertEquals("INGESTING", again.get("status").asText());
        assertTrue(again.get("error").isNull());
        assertEquals("LEC_IMPORT", again.get("stages").get(0).get("kind").asText());
        postJson("/api/v1/lectures/" + failed + "/retry", "", 400);                       // 已经在跑了
        postJson("/api/v1/lectures/" + lecture(c + 1, "READY", KEY) + "/retry", "", 400);
        postJson("/api/v1/lectures/999999999/retry", "", 404);
    }

    @Test
    void aReadyLectureShowsItsConceptsTranscriptAndFrames() throws Exception {
        long id = lecture(course(), "READY", KEY);
        mvc.perform(get("/api/v1/lectures/" + id)).andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("READY"))
                .andExpect(jsonPath("$.concepts[0].name").value("Pre-training"));
        when(mediaProxy.get("/media/" + KEY + "/transcript")).thenReturn(new MediaProxy.Body(200, "application/json",
                "{\"utterances\":[{\"startMs\":0,\"endMs\":4200,\"text\":\"Hi.\"}]}".getBytes(StandardCharsets.UTF_8)));
        mvc.perform(get("/api/v1/lectures/" + id + "/transcript")).andExpect(status().isOk())
                .andExpect(jsonPath("$.utterances[0].text").value("Hi."));
        when(mediaProxy.get("/media/" + KEY + "/frames/000020000.jpg"))
                .thenReturn(new MediaProxy.Body(200, "image/jpeg", new byte[]{1, 2, 3}));
        mvc.perform(get("/api/v1/lectures/" + id + "/frames/000020000.jpg")).andExpect(status().isOk())
                .andExpect(content().contentType("image/jpeg")).andExpect(content().bytes(new byte[]{1, 2, 3}));
        mvc.perform(get("/api/v1/lectures/" + id + "/frames/..%2Fmeta.json")).andExpect(status().is4xxClientError());
        mvc.perform(get("/api/v1/lectures/" + id + "/frames/meta.json")).andExpect(status().isBadRequest());
    }

    @Test
    void aLectureThatWasNeverImportedHasNoTranscript() throws Exception {
        long id = lecture(course(), "FAILED", null);
        mvc.perform(get("/api/v1/lectures/" + id + "/transcript")).andExpect(status().isNotFound());
        mvc.perform(get("/api/v1/lectures/" + id + "/frames")).andExpect(status().isNotFound());
    }

    @Test
    void aLectureStuckInIngestingWithNoLiveTaskCanBeRetried() throws Exception {
        long c = course();
        long orphan = lecture(c, "INGESTING", null);       // 进程死在"插入讲次"和"提交任务"之间:讲次在,任务没有
        JsonNode again = postJson("/api/v1/lectures/" + orphan + "/retry", "", 200);
        assertEquals("LEC_IMPORT", again.get("stages").get(0).get("kind").asText());
        postJson("/api/v1/lectures/" + orphan + "/retry", "", 400);                     // 现在有在跑的任务了

        TaskEntity finished = new TaskEntity();            // 任务早就落定了,但收尾时没能把状态写回讲次
        finished.setTaskUuid(java.util.UUID.randomUUID().toString());
        finished.setType("LECTURE_INGEST");
        finished.setStatus("FAILED");
        finished.setSubtaskTotal(0);
        finished.setSubtaskDone(0);
        finished.setSubtaskFailed(0);
        taskMapper.insert(finished);
        long stuck = lecture(c + 1, "INGESTING", null);
        LectureEntity l = lectureMapper.selectById(stuck);
        l.setTaskUuid(finished.getTaskUuid());
        lectureMapper.updateById(l);
        assertEquals("INGESTING", postJson("/api/v1/lectures/" + stuck + "/retry", "", 200).get("status").asText());
    }
}
