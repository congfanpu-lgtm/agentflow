package com.agentflow.common.trace;

public class TraceEvent {
    private String traceId;   // = taskUuid
    private Long taskId;
    private Long subtaskId;   // 可空
    private String stage;     // TraceStage.name()
    private String status;
    private String detail;
    private long timestamp;   // epoch millis

    public TraceEvent() {}

    public TraceEvent(String traceId, Long taskId, Long subtaskId,
                      String stage, String status, String detail, long timestamp) {
        this.traceId = traceId; this.taskId = taskId; this.subtaskId = subtaskId;
        this.stage = stage; this.status = status; this.detail = detail; this.timestamp = timestamp;
    }

    public String getTraceId() { return traceId; }
    public void setTraceId(String traceId) { this.traceId = traceId; }
    public Long getTaskId() { return taskId; }
    public void setTaskId(Long taskId) { this.taskId = taskId; }
    public Long getSubtaskId() { return subtaskId; }
    public void setSubtaskId(Long subtaskId) { this.subtaskId = subtaskId; }
    public String getStage() { return stage; }
    public void setStage(String stage) { this.stage = stage; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getDetail() { return detail; }
    public void setDetail(String detail) { this.detail = detail; }
    public long getTimestamp() { return timestamp; }
    public void setTimestamp(long timestamp) { this.timestamp = timestamp; }
}
