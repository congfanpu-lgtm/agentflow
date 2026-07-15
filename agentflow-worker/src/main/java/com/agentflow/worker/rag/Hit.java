package com.agentflow.worker.rag;

/** RAG 检索命中项:doc id、原文、余弦相似分。 */
public record Hit(String id, String text, double score) {}
