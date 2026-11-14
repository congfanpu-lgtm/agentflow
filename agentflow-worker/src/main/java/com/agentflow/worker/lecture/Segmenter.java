package com.agentflow.worker.lecture;

import com.fasterxml.jackson.databind.JsonNode;

import java.util.ArrayList;
import java.util.List;

/**
 * 把一讲课切成检索用的片段。纯函数,不依赖 Spring。
 *
 * <p>语音:依次把 utterance 加进当前片段,跨度到 {@link #TARGET_MS} 且这一条以句末标点结尾就收尾,
 * 到 {@link #MAX_MS} 无条件收尾。下一个片段带上前一个片段最后 {@link #OVERLAP_MS} 之内的内容,
 * 这样跨在边界上的一句话在两个片段里都能搜到。
 * 两条 utterance 之间停顿超过 {@link #OVERLAP_MS} 时片段也到此为止,不带重叠。
 * 上限 90 秒是为了不超过向量模型的 512 token 窗口(讲课约每分钟 155 个词)。
 *
 * <p>幻灯片:每张截到的帧是一个片段,从它出现到下一张出现。
 */
public final class Segmenter {

    public static final long TARGET_MS = 45_000;
    public static final long MAX_MS = 90_000;
    public static final long OVERLAP_MS = 10_000;
    private static final String SENTENCE_END = ".?!。？！";

    public record Segment(long startMs, long endMs, String kind, String text) {
    }

    private record Utterance(long startMs, long endMs, String text) {
    }

    private Segmenter() {
    }

    /** utterances:媒体服务逐字稿里的数组,每条 {"startMs","endMs","text"},按时间排好。 */
    public static List<Segment> speech(JsonNode utterances) {
        List<Segment> out = new ArrayList<>();
        List<Utterance> current = new ArrayList<>();
        int carried = 0;   // current 开头有几条是从上一个片段带过来的
        for (JsonNode node : utterances) {
            String text = node.path("text").asText("").strip();
            if (text.isEmpty()) {
                continue;
            }
            long startMs = node.path("startMs").asLong();
            // 长时间没人说话(课间休息、放视频):片段到此为止,重叠也不带过去,否则一个片段会横跨整段空白
            if (!current.isEmpty() && startMs - current.get(current.size() - 1).endMs() > OVERLAP_MS) {
                if (current.size() > carried) {
                    out.add(segment(current));
                }
                current = new ArrayList<>();
                carried = 0;
            }
            current.add(new Utterance(startMs, node.path("endMs").asLong(), text));
            long span = current.get(current.size() - 1).endMs() - current.get(0).startMs();
            boolean sentenceEnd = SENTENCE_END.indexOf(text.charAt(text.length() - 1)) >= 0;
            if (span >= MAX_MS || (span >= TARGET_MS && sentenceEnd)) {
                out.add(segment(current));
                current = overlapTail(current);
                carried = current.size();
            }
        }
        if (current.size() > carried) {   // 只剩带过来的重叠内容就不单独成段
            out.add(segment(current));
        }
        return out;
    }

    /** frames:媒体服务帧列表里的数组,每帧 {"tsMs","text"};最后一帧持续到 durationMs。 */
    public static List<Segment> slides(JsonNode frames, long durationMs) {
        List<Segment> out = new ArrayList<>();
        for (int i = 0; i < frames.size(); i++) {
            long start = frames.get(i).path("tsMs").asLong();
            long end = i + 1 < frames.size() ? frames.get(i + 1).path("tsMs").asLong() : Math.max(durationMs, start);
            out.add(new Segment(start, end, "slide", frames.get(i).path("text").asText("")));
        }
        return out;
    }

    private static Segment segment(List<Utterance> utterances) {
        List<String> texts = utterances.stream().map(Utterance::text).toList();
        return new Segment(utterances.get(0).startMs(), utterances.get(utterances.size() - 1).endMs(),
                "speech", String.join(" ", texts));
    }

    /**
     * 片段末尾 OVERLAP_MS 之内开始的那几条(按经过的时间算,不是按说话的时长:稀疏的短句之间隔着停顿,
     * 按说话时长算会把很久以前的句子也带上)。最后一条本身就比 OVERLAP_MS 长,就不重叠。
     */
    private static List<Utterance> overlapTail(List<Utterance> segment) {
        long end = segment.get(segment.size() - 1).endMs();
        int from = segment.size();
        while (from > 0 && end - segment.get(from - 1).startMs() <= OVERLAP_MS) {
            from--;
        }
        return new ArrayList<>(segment.subList(from, segment.size()));
    }
}
