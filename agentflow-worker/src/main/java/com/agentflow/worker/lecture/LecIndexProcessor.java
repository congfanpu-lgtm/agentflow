package com.agentflow.worker.lecture;

import com.agentflow.worker.lecture.Segmenter.Segment;
import com.agentflow.worker.media.MediaClient;
import com.agentflow.worker.processor.SubtaskProcessor;
import com.agentflow.worker.rag.RagClient;
import com.agentflow.worker.retry.NonRetryableException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * 讲次摄入第 4 步:逐字稿切成语音片段、幻灯片文字各成一个片段,写进检索服务。
 * 那边是整讲替换,所以这一步重复执行(重试、超时重投)不会留下两份片段。
 */
@Component
@RequiredArgsConstructor
public class LecIndexProcessor implements SubtaskProcessor {

    private static final ObjectMapper OM = new ObjectMapper();
    private final MediaClient media;
    private final RagClient rag;

    @Override
    public String type() {
        return "LEC_INDEX";
    }

    @Override
    public String process(String inputJson) throws Exception {
        JsonNode in = OM.readTree(inputJson);
        String mediaKey = in.path("mediaKey").asText();
        List<Segment> segments = new ArrayList<>(Segmenter.speech(media.transcript(mediaKey).path("utterances")));
        int speech = segments.size();
        if (speech == 0) {
            throw new NonRetryableException("这一讲没有识别出语音");   // 再转写一遍也是空的
        }
        int slides = 0;
        if (in.path("withSlides").asBoolean()) {
            List<Segment> slideSegments = Segmenter.slides(media.frameList(mediaKey).path("frames"),
                    in.path("durationMs").asLong());
            slides = slideSegments.size();
            segments.addAll(slideSegments);
        }
        rag.indexSegments(in.path("courseId").asLong(), in.path("lectureId").asLong(), segments);
        return OM.createObjectNode().put("speech", speech).put("slides", slides).toString();
    }
}
