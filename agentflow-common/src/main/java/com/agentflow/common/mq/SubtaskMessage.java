package com.agentflow.common.mq;

public class SubtaskMessage {
    private Long taskId;
    private Long subtaskId;
    private String subtaskUuid;
    private String type;
    private int seq;
    private String inputJson;

    public SubtaskMessage() {}

    public SubtaskMessage(Long taskId, Long subtaskId, String subtaskUuid,
                          String type, int seq, String inputJson) {
        this.taskId = taskId;
        this.subtaskId = subtaskId;
        this.subtaskUuid = subtaskUuid;
        this.type = type;
        this.seq = seq;
        this.inputJson = inputJson;
    }

    public Long getTaskId() { return taskId; }
    public void setTaskId(Long taskId) { this.taskId = taskId; }
    public Long getSubtaskId() { return subtaskId; }
    public void setSubtaskId(Long subtaskId) { this.subtaskId = subtaskId; }
    public String getSubtaskUuid() { return subtaskUuid; }
    public void setSubtaskUuid(String subtaskUuid) { this.subtaskUuid = subtaskUuid; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public int getSeq() { return seq; }
    public void setSeq(int seq) { this.seq = seq; }
    public String getInputJson() { return inputJson; }
    public void setInputJson(String inputJson) { this.inputJson = inputJson; }
}
