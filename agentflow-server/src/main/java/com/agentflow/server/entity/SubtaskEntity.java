package com.agentflow.server.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("subtask")
public class SubtaskEntity {
    @TableId(type = IdType.AUTO)
    private Long id;
    private String subtaskUuid;
    private Long taskId;
    private Integer seq;
    private String status;       // SubtaskStatus.name()
    private String input;        // JSON 字符串
    private String output;       // JSON 字符串
    private String errorMsg;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
