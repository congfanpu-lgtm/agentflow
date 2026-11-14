package com.agentflow.worker.lecture;

import com.agentflow.worker.lecture.Segmenter.Segment;
import com.agentflow.worker.media.MediaClient;
import com.agentflow.worker.rag.RagClient;
import com.agentflow.worker.retry.NonRetryableException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class LecProcessorsTest {

    private static final ObjectMapper OM = new ObjectMapper();
    private static final String KEY = "0123456789abcdef0123456789abcdef";
    private final MediaClient media = mock(MediaClient.class);
    private final RagClient rag = mock(RagClient.class);

    private static JsonNode json(String s) throws Exception {
        return OM.readTree(s);
    }

    @Test
    void importPassesTheLinkAndReportsWhatWasStored() throws Exception {
        when(media.importMedia("https://youtu.be/abc", null)).thenReturn(json(
                "{\"mediaKey\":\"" + KEY + "\",\"title\":\"LLM Agents Overview\",\"durationMs\":7101000,"
                        + "\"audio\":\"audio.m4a\",\"video\":\"video.mp4\",\"cached\":false}"));
        LecImportProcessor p = new LecImportProcessor(media);
        assertEquals("LEC_IMPORT", p.type());
        JsonNode out = json(p.process("{\"lectureId\":7,\"url\":\"https://youtu.be/abc\"}"));
        assertEquals(KEY, out.get("mediaKey").asText());
        assertEquals("LLM Agents Overview", out.get("title").asText());
        assertEquals(7101000, out.get("durationMs").asLong());
        assertTrue(out.get("hasVideo").asBoolean());
    }

    @Test
    void importOfALocalFileSendsThePath() throws Exception {
        when(media.importMedia(null, "lec1.wav")).thenReturn(json(
                "{\"mediaKey\":\"" + KEY + "\",\"title\":\"lec1\",\"durationMs\":2000,\"video\":null}"));
        JsonNode out = json(new LecImportProcessor(media).process("{\"lectureId\":7,\"path\":\"lec1.wav\"}"));
        assertFalse(out.get("hasVideo").asBoolean());
    }

    @Test
    void transcribeAndFramesReportCounts() throws Exception {
        when(media.transcribe(KEY)).thenReturn(json("{\"mediaKey\":\"" + KEY + "\",\"utterances\":1298,\"cached\":false}"));
        when(media.frames(KEY)).thenReturn(json("{\"mediaKey\":\"" + KEY + "\",\"frames\":120,\"cached\":true}"));
        String in = "{\"lectureId\":7,\"mediaKey\":\"" + KEY + "\"}";
        LecTranscribeProcessor t = new LecTranscribeProcessor(media);
        LecFramesProcessor f = new LecFramesProcessor(media);
        assertEquals("LEC_TRANSCRIBE", t.type());
        assertEquals("LEC_FRAMES", f.type());
        assertEquals(1298, json(t.process(in)).get("utterances").asInt());
        JsonNode frames = json(f.process(in));
        assertEquals(120, frames.get("frames").asInt());
        assertTrue(frames.get("cached").asBoolean());
    }

    private void transcript(String utterances) throws Exception {
        when(media.transcript(KEY)).thenReturn(json("{\"model\":\"large-v3\",\"language\":\"en\",\"utterances\":" + utterances + "}"));
    }

    private static String indexInput(boolean withSlides) {
        return "{\"courseId\":3,\"lectureId\":7,\"mediaKey\":\"" + KEY + "\",\"durationMs\":60000,\"withSlides\":" + withSlides + "}";
    }

    @Test
    @SuppressWarnings("unchecked")
    void indexSendsSpeechAndSlideSegmentsOfTheLecture() throws Exception {
        transcript("[{\"startMs\":0,\"endMs\":5000,\"text\":\"Welcome.\"},{\"startMs\":5000,\"endMs\":9000,\"text\":\"Today: agents.\"}]");
        when(media.frameList(KEY)).thenReturn(json("{\"frames\":[{\"tsMs\":0,\"file\":\"frames/000000000.jpg\",\"text\":\"Title slide\"},"
                + "{\"tsMs\":20000,\"file\":\"frames/000020000.jpg\",\"text\":\"Agenda\"}]}"));
        when(rag.indexSegments(anyLong(), anyLong(), anyList())).thenReturn(3);
        LecIndexProcessor p = new LecIndexProcessor(media, rag);
        assertEquals("LEC_INDEX", p.type());
        JsonNode out = json(p.process(indexInput(true)));
        assertEquals(1, out.get("speech").asInt());
        assertEquals(2, out.get("slides").asInt());
        ArgumentCaptor<List<Segment>> segs = ArgumentCaptor.forClass(List.class);
        verify(rag).indexSegments(eq(3L), eq(7L), segs.capture());
        assertEquals(List.of(new Segment(0, 9000, "speech", "Welcome. Today: agents."),
                new Segment(0, 20000, "slide", "Title slide"), new Segment(20000, 60000, "slide", "Agenda")), segs.getValue());
    }

    @Test
    void indexWithoutSlidesNeverAsksForFrames() throws Exception {
        transcript("[{\"startMs\":0,\"endMs\":5000,\"text\":\"Welcome.\"}]");
        when(rag.indexSegments(anyLong(), anyLong(), anyList())).thenReturn(1);
        JsonNode out = json(new LecIndexProcessor(media, rag).process(indexInput(false)));
        assertEquals(0, out.get("slides").asInt());
        verify(media, never()).frameList(any());
    }

    @Test
    void indexFailsForGoodWhenNothingWasSaid() throws Exception {
        transcript("[]");
        assertThrows(NonRetryableException.class, () -> new LecIndexProcessor(media, rag).process(indexInput(true)));
        verify(rag, never()).indexSegments(anyLong(), anyLong(), anyList());
    }
}
