package com.agentflow.worker.lecture;

import com.agentflow.worker.media.MediaClient;
import com.agentflow.worker.processor.SubtaskProcessor;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/** 讲次摄入第 3 步(和转写并行):截出幻灯片并识别上面的文字。失败不拖垮整讲,只是索引里没有幻灯片。 */
@Component
@RequiredArgsConstructor
public class LecFramesProcessor implements SubtaskProcessor {

    private static final ObjectMapper OM = new ObjectMapper();
    private final MediaClient media;

    @Override
    public String type() {
        return "LEC_FRAMES";
    }

    @Override
    public String process(String inputJson) throws Exception {
        JsonNode r = media.frames(OM.readTree(inputJson).path("mediaKey").asText());
        return OM.createObjectNode()
                .put("frames", r.path("frames").asInt())
                .put("cached", r.path("cached").asBoolean())
                .toString();
    }
}
