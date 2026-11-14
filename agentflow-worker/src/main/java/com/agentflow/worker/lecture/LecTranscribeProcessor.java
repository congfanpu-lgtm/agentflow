package com.agentflow.worker.lecture;

import com.agentflow.worker.media.MediaClient;
import com.agentflow.worker.processor.SubtaskProcessor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * 讲次摄入第 2 步:转写。一讲一个子任务,整讲交给媒体服务;那边同一时刻只跑一路(一张显卡上多路并行没有收益)。
 * 逐字稿留在媒体服务里,这里只回传条数。
 */
@Component
@RequiredArgsConstructor
public class LecTranscribeProcessor implements SubtaskProcessor {

    private static final ObjectMapper OM = new ObjectMapper();
    private final MediaClient media;

    @Override
    public String type() {
        return "LEC_TRANSCRIBE";
    }

    @Override
    public String process(String inputJson) throws Exception {
        JsonNode r = media.transcribe(OM.readTree(inputJson).path("mediaKey").asText());
        return OM.createObjectNode()
                .put("utterances", r.path("utterances").asInt())
                .put("cached", r.path("cached").asBoolean())
                .toString();
    }
}
