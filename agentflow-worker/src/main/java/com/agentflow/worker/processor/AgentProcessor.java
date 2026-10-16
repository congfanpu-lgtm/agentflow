package com.agentflow.worker.processor;

import com.agentflow.common.mq.SubtaskMessage;
import com.agentflow.worker.agent.AgentLoop;
import com.agentflow.worker.agent.AgentRun;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/** RESEARCH_AGENT 阶段②:一个子问题一个 ReAct agent。需要 subtaskUuid 做存档 key,故走 process(SubtaskMessage)。 */
@Component
@RequiredArgsConstructor
public class AgentProcessor implements SubtaskProcessor {

    private static final ObjectMapper OM = new ObjectMapper();

    private final AgentLoop loop;

    @Override
    public String type() {
        return "AGENT";
    }

    @Override
    public String process(String inputJson) {
        throw new UnsupportedOperationException("AGENT 需要子任务身份(按 subtaskUuid 存档续跑),请走 process(SubtaskMessage)");
    }

    @Override
    public String process(SubtaskMessage msg) throws Exception {
        JsonNode in = OM.readTree(msg.getInputJson());
        String question = in.path("question").asText("").trim();
        if (question.isEmpty()) {
            throw new IllegalArgumentException("input.question 缺失");
        }
        String model = in.hasNonNull("model") ? in.get("model").asText() : null;
        return loop.run(new AgentRun(msg.getTaskId(), msg.getSubtaskId(), msg.getSubtaskUuid(), question, model))
                .toString();
    }
}
