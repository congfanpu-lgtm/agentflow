package com.agentflow.server.trace;

import org.springframework.data.mongodb.repository.MongoRepository;
import java.util.List;

public interface TraceEventRepository extends MongoRepository<TraceDocument, String> {
    List<TraceDocument> findByTraceIdOrderByTimestampAsc(String traceId);
}
