package com.agentflow.common.mq;

public final class Topics {
    public static final String SUBTASK = "AGENTFLOW_SUBTASK";
    public static final String RESULT = "AGENTFLOW_RESULT";
    public static final String RETRY_5S = "AGENTFLOW_RETRY_5S";
    public static final String RETRY_30S = "AGENTFLOW_RETRY_30S";
    public static final String RETRY_5M = "AGENTFLOW_RETRY_5M";
    public static final String DLQ = "AGENTFLOW_DLQ";

    public static final String RETRY_ATTEMPT_HEADER = "x-retry-attempt";
    public static final String NOT_BEFORE_HEADER = "x-not-before"; // epoch millis

    private Topics() {}
}
