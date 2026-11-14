package com.agentflow.worker.lecture;

import com.agentflow.worker.media.MediaClient;
import com.agentflow.worker.processor.SubtaskProcessor;
import com.agentflow.worker.retry.NonRetryableException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/** 讲次摄入第 1 步:让媒体服务把这一讲拿到手(YouTube 链接或 inbox 里的文件)。同一内容那边只存一份。 */
@Component
@RequiredArgsConstructor
public class LecImportProcessor implements SubtaskProcessor {

    private static final ObjectMapper OM = new ObjectMapper();
    private final MediaClient media;

    @Override
    public String type() {
        return "LEC_IMPORT";
    }

    @Override
    public String process(String inputJson) throws Exception {
        JsonNode in = OM.readTree(inputJson);
        String url = in.hasNonNull("url") ? in.get("url").asText() : null;
        String path = in.hasNonNull("path") ? in.get("path").asText() : null;
        if (url == null && path == null) {
            throw new NonRetryableException("讲次既没有链接也没有文件");
        }
        JsonNode meta = media.importMedia(url, path);
        return OM.createObjectNode()
                .put("mediaKey", meta.path("mediaKey").asText())
                .put("title", meta.path("title").asText())
                .put("durationMs", meta.path("durationMs").asLong())
                .put("hasVideo", meta.hasNonNull("video"))
                .toString();
    }
}
