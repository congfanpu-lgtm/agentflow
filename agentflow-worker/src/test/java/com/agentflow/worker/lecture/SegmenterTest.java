package com.agentflow.worker.lecture;

import com.agentflow.worker.lecture.Segmenter.Segment;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SegmenterTest {

    private static final ObjectMapper OM = new ObjectMapper();

    /** n 条 utterance,每条 10 秒、首尾相接;编号在 sentenceEnds 里的以句号结尾。 */
    private static ArrayNode utterances(int n, Set<Integer> sentenceEnds) {
        ArrayNode arr = OM.createArrayNode();
        for (int i = 0; i < n; i++) {
            arr.addObject().put("startMs", i * 10_000L).put("endMs", (i + 1) * 10_000L)
                    .put("text", "u" + i + (sentenceEnds.contains(i) ? "." : ""));
        }
        return arr;
    }

    @Test
    void cutsAtTheFirstSentenceEndAfterTheTarget() {
        List<Segment> segs = Segmenter.speech(utterances(12, Set.of(1, 5)));
        assertEquals(2, segs.size());
        assertEquals(new Segment(0, 60_000, "speech", "u0 u1. u2 u3 u4 u5."), segs.get(0));   // u1 的句号太早,不切
        assertEquals(50_000, segs.get(1).startMs());                                           // 带上一段的最后 10 秒
        assertEquals(120_000, segs.get(1).endMs());
        assertTrue(segs.get(1).text().startsWith("u5. u6"));
    }

    @Test
    void cutsAtTheHardCapWhenThereIsNoPunctuation() {
        List<Segment> segs = Segmenter.speech(utterances(20, Set.of()));
        assertEquals(List.of(0L, 80_000L, 160_000L), segs.stream().map(Segment::startMs).toList());
        assertEquals(List.of(90_000L, 170_000L, 200_000L), segs.stream().map(Segment::endMs).toList());
        String all = String.join(" ", segs.stream().map(Segment::text).toList());
        for (int i = 0; i < 20; i++) {
            assertTrue((" " + all + " ").contains(" u" + i + " "), "u" + i + " 没有进任何片段");
        }
    }

    @Test
    void aTailThatIsOnlyOverlapIsNotEmitted() {
        assertEquals(1, Segmenter.speech(utterances(6, Set.of(5))).size());
    }

    @Test
    void anUtteranceLongerThanTheCapIsItsOwnSegment() {
        ArrayNode arr = OM.createArrayNode();
        arr.addObject().put("startMs", 0).put("endMs", 200_000).put("text", "a very long monologue");
        arr.addObject().put("startMs", 200_000).put("endMs", 205_000).put("text", "Next.");
        List<Segment> segs = Segmenter.speech(arr);
        assertEquals(new Segment(0, 200_000, "speech", "a very long monologue"), segs.get(0));
        assertEquals(new Segment(200_000, 205_000, "speech", "Next."), segs.get(1));              // 200 秒的那条放不进重叠
    }

    @Test
    void emptyTranscriptGivesNoSegments() {
        assertTrue(Segmenter.speech(OM.createArrayNode()).isEmpty());
        assertTrue(Segmenter.slides(OM.createArrayNode(), 60_000).isEmpty());
    }

    @Test
    void chinesePunctuationAlsoEndsASentence() {
        ArrayNode arr = OM.createArrayNode();
        for (int i = 0; i < 8; i++) {
            arr.addObject().put("startMs", i * 10_000L).put("endMs", (i + 1) * 10_000L)
                    .put("text", i == 4 ? "这是梯度下降。" : "第" + i + "句");
        }
        assertEquals(50_000, Segmenter.speech(arr).get(0).endMs());
    }

    @Test
    void eachSlideLastsUntilTheNextOne() throws Exception {
        List<Segment> segs = Segmenter.slides(OM.readTree(
                "[{\"tsMs\":0,\"file\":\"frames/000000000.jpg\",\"text\":\"Title\"},"
                        + "{\"tsMs\":20000,\"file\":\"frames/000020000.jpg\",\"text\":\"SGD: one sample per step\"}]"), 60_000);
        assertEquals(List.of(new Segment(0, 20_000, "slide", "Title"),
                new Segment(20_000, 60_000, "slide", "SGD: one sample per step")), segs);
    }

    @Test
    void aLongPauseEndsTheSegmentAndNothingIsCarriedAcrossIt() {
        ArrayNode arr = OM.createArrayNode();
        arr.addObject().put("startMs", 0).put("endMs", 10_000).put("text", "before the break");
        arr.addObject().put("startMs", 10_000).put("endMs", 20_000).put("text", "see you in ten minutes");
        arr.addObject().put("startMs", 620_000).put("endMs", 630_000).put("text", "Welcome back.");
        assertEquals(List.of(new Segment(0, 20_000, "speech", "before the break see you in ten minutes"),
                new Segment(620_000, 630_000, "speech", "Welcome back.")), Segmenter.speech(arr));
    }
}
