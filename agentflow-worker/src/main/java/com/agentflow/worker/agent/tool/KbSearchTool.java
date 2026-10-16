package com.agentflow.worker.agent.tool;

import com.agentflow.worker.rag.Hit;
import com.agentflow.worker.rag.RagClient;
import com.agentflow.worker.rag.RagProperties;
import com.fasterxml.jackson.databind.JsonNode;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

/** 检索本地知识库(复用现有 RAG 服务)。来源以 kb://<docId> 标注,供引用校验使用。 */
@Component
@RequiredArgsConstructor
public class KbSearchTool implements AgentTool {

    private final RagClient ragClient;
    private final RagProperties props;

    @Override
    public String name() {
        return "kb_search";
    }

    @Override
    public String description() {
        return "检索本地知识库(项目文档、运维笔记),返回最相关的片段及来源。";
    }

    @Override
    public String parametersSchema() {
        return "{\"type\":\"object\",\"properties\":{\"query\":{\"type\":\"string\",\"description\":\"检索内容\"}},"
                + "\"required\":[\"query\"]}";
    }

    @Override
    public String run(JsonNode args) {
        String q = args.path("query").asText("").trim();
        if (q.isEmpty()) {
            throw new IllegalArgumentException("query 不能为空");
        }
        List<Hit> hits = ragClient.search(q, props.getTopK());
        if (hits.isEmpty()) {
            return "知识库没有相关内容:" + q;
        }
        StringBuilder sb = new StringBuilder();
        for (Hit h : hits) {
            sb.append("来源: kb://").append(h.id()).append('\n').append(h.text()).append("\n\n");
        }
        return sb.toString().trim();
    }
}
