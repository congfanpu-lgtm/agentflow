package com.agentflow.common.mq;

public class ResultMessage {
    private Long taskId;
    private Long subtaskId;
    private boolean success;
    private String outputJson;
    private String errorMsg;

    public ResultMessage() {}

    public ResultMessage(Long taskId, Long subtaskId, boolean success,
                         String outputJson, String errorMsg) {
        this.taskId = taskId;
        this.subtaskId = subtaskId;
        this.success = success;
        this.outputJson = outputJson;
        this.errorMsg = errorMsg;
    }

    public Long getTaskId() { return taskId; }
    public void setTaskId(Long taskId) { this.taskId = taskId; }
    public Long getSubtaskId() { return subtaskId; }
    public void setSubtaskId(Long subtaskId) { this.subtaskId = subtaskId; }
    public boolean isSuccess() { return success; }
    public void setSuccess(boolean success) { this.success = success; }
    public String getOutputJson() { return outputJson; }
    public void setOutputJson(String outputJson) { this.outputJson = outputJson; }
    public String getErrorMsg() { return errorMsg; }
    public void setErrorMsg(String errorMsg) { this.errorMsg = errorMsg; }
}
