package com.agentflow.server.coordinator;

import com.agentflow.common.state.TaskStatus;
import com.agentflow.server.entity.LectureEntity;
import com.agentflow.server.entity.SubtaskEntity;
import com.agentflow.server.entity.TaskEntity;
import com.agentflow.server.mapper.LectureMapper;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LectureIngestDecomposerTest {

    private static final String KEY = "0123456789abcdef0123456789abcdef";
    private static final String IMPORTED = "{\"mediaKey\":\"" + KEY + "\",\"title\":\"LLM Agents Overview\","
            + "\"durationMs\":7101000,\"hasVideo\":true}";
    private static final String CONCEPTS = "{\"concepts\":[{\"name\":\"Pre-training\",\"startMs\":0,\"summary\":\"s\"}],\"usage\":{}}";

    private final ObjectMapper om = new ObjectMapper();
    private final LectureMapper lectures = mock(LectureMapper.class);
    private final LectureIngestDecomposer d = new LectureIngestDecomposer(om, lectures);

    private LectureEntity lecture(String url, String path, String title) {
        LectureEntity l = new LectureEntity();
        l.setId(7L);
        l.setCourseId(3L);
        l.setSourceUrl(url);
        l.setSourcePath(path);
        l.setTitle(title);
        l.setStatus("INGESTING");
        when(lectures.selectById(7L)).thenReturn(l);
        return l;
    }

    private static SubtaskEntity sub(int seq, String kind, String status, String output, String error) {
        SubtaskEntity s = new SubtaskEntity();
        s.setSeq(seq);
        s.setKind(kind);
        s.setStatus(status);
        s.setOutput(output);
        s.setErrorMsg(error);
        return s;
    }

    private static SubtaskEntity ok(int seq, String kind, String output) {
        return sub(seq, kind, "COMPLETED", output, null);
    }

    private static SubtaskEntity failed(int seq, String kind, String error) {
        return sub(seq, kind, "FAILED", null, error);
    }

    private static TaskEntity task(String status) {
        TaskEntity t = new TaskEntity();
        t.setType("LECTURE_INGEST");
        t.setStatus(status);
        t.setPayload("{\"lectureId\":7,\"courseId\":3}");
        return t;
    }

    /** 前 n 步都成功的子任务列表(n 最多 5)。 */
    private static List<SubtaskEntity> through(int n) {
        List<SubtaskEntity> all = List.of(ok(0, "LEC_IMPORT", IMPORTED), ok(1, "LEC_TRANSCRIBE", "{\"utterances\":1298}"),
                ok(2, "LEC_FRAMES", "{\"frames\":120}"), ok(3, "LEC_INDEX", "{\"speech\":150,\"slides\":120}"),
                ok(4, "LEC_CONCEPTS", CONCEPTS));
        return new ArrayList<>(all.subList(0, n));
    }

    private JsonNode in(SubtaskDef def) throws Exception {
        return om.readTree(def.inputJson());
    }

    @Test
    void decomposeIgnoresALinkInThePayload() throws Exception {
        lecture("https://youtu.be/abc", null, null);
        List<SubtaskDef> defs = d.decompose(om.readTree("{\"lectureId\":7,\"courseId\":3,\"url\":\"https://evil.example/x\"}"));
        assertEquals(1, defs.size());
        assertEquals("LEC_IMPORT", defs.get(0).kind());
        assertEquals(0, defs.get(0).seq());
        assertEquals("https://youtu.be/abc", in(defs.get(0)).get("url").asText());
        assertEquals(7, in(defs.get(0)).get("lectureId").asLong());
        assertFalse(in(defs.get(0)).has("path"));
    }

    @Test
    void decomposeUsesThePathOfALocalFile() throws Exception {
        lecture(null, "lec1.mp4", null);
        JsonNode input = in(d.decompose(om.readTree("{\"lectureId\":7,\"courseId\":3}")).get(0));
        assertEquals("lec1.mp4", input.get("path").asText());
        assertFalse(input.has("url"));
    }

    @Test
    void decomposeRejectsWhatIsNotAKnownLecture() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> d.decompose(null));
        assertThrows(IllegalArgumentException.class, () -> d.decompose(om.readTree("{}")));
        assertThrows(IllegalArgumentException.class, () -> d.decompose(om.readTree("{\"lectureId\":8,\"courseId\":3}")));
        lecture("https://youtu.be/abc", null, null);
        assertThrows(IllegalArgumentException.class, () -> d.decompose(om.readTree("{\"lectureId\":7,\"courseId\":4}")));
    }

    @Test
    void afterImportTranscriptionAndFramesRunSideBySide() throws Exception {
        List<SubtaskDef> next = d.nextStage(task("RUNNING"), through(1));
        assertEquals(List.of("LEC_TRANSCRIBE", "LEC_FRAMES"), next.stream().map(SubtaskDef::kind).toList());
        assertEquals(List.of(1, 2), next.stream().map(SubtaskDef::seq).toList());
        assertEquals(KEY, in(next.get(0)).get("mediaKey").asText());
        assertEquals(KEY, in(next.get(1)).get("mediaKey").asText());
    }

    @Test
    void theIndexIsBuiltWithSlidesOnlyWhenFramesSucceeded() throws Exception {
        List<SubtaskDef> next = d.nextStage(task("RUNNING"), through(3));
        assertEquals(List.of("LEC_INDEX"), next.stream().map(SubtaskDef::kind).toList());
        JsonNode input = in(next.get(0));
        assertEquals(3, next.get(0).seq());
        assertEquals(3, input.get("courseId").asLong());
        assertEquals(7, input.get("lectureId").asLong());
        assertEquals(KEY, input.get("mediaKey").asText());
        assertEquals(7101000, input.get("durationMs").asLong());
        assertTrue(input.get("withSlides").asBoolean());

        List<SubtaskEntity> framesFailed = through(2);
        framesFailed.add(failed(2, "LEC_FRAMES", "media 500: OCR crashed"));
        assertFalse(in(d.nextStage(task("RUNNING"), framesFailed).get(0)).get("withSlides").asBoolean());
    }

    @Test
    void afterTheIndexConceptsAreExtracted() throws Exception {
        List<SubtaskDef> next = d.nextStage(task("RUNNING"), through(4));
        assertEquals(List.of("LEC_CONCEPTS"), next.stream().map(SubtaskDef::kind).toList());
        assertEquals(4, next.get(0).seq());
        assertEquals(KEY, in(next.get(0)).get("mediaKey").asText());
        assertEquals("LLM Agents Overview", in(next.get(0)).get("title").asText());
    }

    @Test
    void aFailedStepThatOthersNeedEndsTheTask() {
        assertTrue(d.nextStage(task("RUNNING"), List.of(failed(0, "LEC_IMPORT", "media 400: bad link"))).isEmpty());
        List<SubtaskEntity> noTranscript = through(1);
        noTranscript.add(failed(1, "LEC_TRANSCRIBE", "CUDA out of memory"));
        noTranscript.add(ok(2, "LEC_FRAMES", "{\"frames\":120}"));
        assertTrue(d.nextStage(task("RUNNING"), noTranscript).isEmpty());
        List<SubtaskEntity> noIndex = through(3);
        noIndex.add(failed(3, "LEC_INDEX", "这一讲没有识别出语音"));
        assertTrue(d.nextStage(task("RUNNING"), noIndex).isEmpty());
        assertTrue(d.nextStage(task("RUNNING"), through(5)).isEmpty());
    }

    @Test
    void terminalStatusFollowsTheIndex() {
        assertEquals(TaskStatus.COMPLETED, d.terminalStatus(task("RUNNING"), through(5), TaskStatus.COMPLETED));
        List<SubtaskEntity> noSlides = through(2);
        noSlides.add(failed(2, "LEC_FRAMES", "x"));
        noSlides.add(ok(3, "LEC_INDEX", "{\"speech\":150,\"slides\":0}"));
        noSlides.add(ok(4, "LEC_CONCEPTS", CONCEPTS));
        assertEquals(TaskStatus.PARTIAL_FAILED, d.terminalStatus(task("RUNNING"), noSlides, TaskStatus.PARTIAL_FAILED));
        List<SubtaskEntity> noConcepts = through(4);
        noConcepts.add(failed(4, "LEC_CONCEPTS", "x"));
        assertEquals(TaskStatus.PARTIAL_FAILED, d.terminalStatus(task("RUNNING"), noConcepts, TaskStatus.PARTIAL_FAILED));
        List<SubtaskEntity> noTranscript = through(1);
        noTranscript.add(failed(1, "LEC_TRANSCRIBE", "x"));
        noTranscript.add(ok(2, "LEC_FRAMES", "{\"frames\":120}"));
        assertEquals(TaskStatus.FAILED, d.terminalStatus(task("RUNNING"), noTranscript, TaskStatus.PARTIAL_FAILED),
                "导入和截帧成功了也没用:没有逐字稿就没有索引");
    }

    private LectureEntity saved() {
        ArgumentCaptor<LectureEntity> c = ArgumentCaptor.forClass(LectureEntity.class);
        verify(lectures).updateById(c.capture());
        return c.getValue();
    }

    @Test
    void onFinishedMakesTheLectureReady() throws Exception {
        lecture("https://youtu.be/abc", null, null);
        d.onFinished(task("COMPLETED"), through(5));
        LectureEntity l = saved();
        assertEquals("READY", l.getStatus());
        assertEquals(KEY, l.getMediaKey());
        assertEquals(7101000L, l.getDurationMs().longValue());
        assertEquals("LLM Agents Overview", l.getTitle());
        assertEquals("Pre-training", om.readTree(l.getConcepts()).get(0).get("name").asText());
        assertNull(l.getError());
    }

    @Test
    void onFinishedKeepsTheTitleTheUserGave() {
        lecture("https://youtu.be/abc", null, "第 2 讲");
        d.onFinished(task("COMPLETED"), through(5));
        assertEquals("第 2 讲", saved().getTitle());
    }

    @Test
    void onFinishedRecordsWhichStepFailed() {
        lecture("https://youtu.be/abc", null, null);
        List<SubtaskEntity> noSlides = through(2);
        noSlides.add(failed(2, "LEC_FRAMES", "media 500: OCR crashed"));
        noSlides.add(ok(3, "LEC_INDEX", "{\"speech\":150,\"slides\":0}"));
        noSlides.add(ok(4, "LEC_CONCEPTS", CONCEPTS));
        d.onFinished(task("PARTIAL_FAILED"), noSlides);
        LectureEntity l = saved();
        assertEquals("PARTIAL", l.getStatus());
        assertEquals("LEC_FRAMES: media 500: OCR crashed", l.getError());
        assertEquals(KEY, l.getMediaKey());
    }

    @Test
    void failedTranscriptionFailsTheLecture() {
        lecture("https://youtu.be/abc", null, null);
        List<SubtaskEntity> subs = through(1);
        subs.add(failed(1, "LEC_TRANSCRIBE", "CUDA out of memory"));
        subs.add(ok(2, "LEC_FRAMES", "{\"frames\":120}"));
        d.onFinished(task("FAILED"), subs);
        LectureEntity l = saved();
        assertEquals("FAILED", l.getStatus());
        assertEquals("LEC_TRANSCRIBE: CUDA out of memory", l.getError());
        assertNull(l.getConcepts());
    }
}
