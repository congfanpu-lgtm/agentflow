package com.agentflow.worker.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;

/**
 * W1-2 的假 Agent:大写回显 + 模拟耗时。
 * W5-6 换成 LangChain4j 真 Agent 时,只替换这个实现。
 */
@Component
public class EchoProcessor {

    private static final ObjectMapper OM = new ObjectMapper();

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
