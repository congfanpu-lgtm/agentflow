package com.agentflow.worker.rag;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** RAG 检索服务(Python/FastAPI)连接配置。 */
@Getter
@Setter
@ConfigurationProperties("rag")
public class RagProperties {
    private String baseUrl = "http://localhost:8000";
    private int topK = 3;
    private int timeoutMs = 3000;
    /** 写入一讲的片段要先算几百个向量,比检索慢得多。 */
    private int indexTimeoutMs = 120_000;
}
