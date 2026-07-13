package com.agentflow.worker.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

/**
 * ECHO_BATCH 的假 Agent:大写回显 + 模拟耗时。W5-6 起与真 Agent {@code LlmProcessor} 并存,
 * 由 {@code SubtaskListener} 按 type 路由;保留用于回归与 mock 对照。
 */
@Component
public class EchoProcessor implements SubtaskProcessor {

    private static final ObjectMapper OM = new ObjectMapper();

    @Override
    public String type() {
        return "ECHO_BATCH";
    }

    @Override
    public String process(String inputJson) throws Exception {
        JsonNode input = OM.readTree(inputJson);
        JsonNode text = input.get("text");
        if (text == null || text.asText().isEmpty()) {
            throw new IllegalArgumentException("input.text 缺失");
        }
        Thread.sleep(300);  // 模拟真实工作耗时
        String echoed = text.asText().toUpperCase();
        var out = OM.createObjectNode();
        out.put("echo", echoed);
        out.put("length", echoed.length());
        return OM.writeValueAsString(out);
    }
}
