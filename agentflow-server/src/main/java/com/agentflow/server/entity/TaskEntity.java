package com.agentflow.server.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("task")
public class TaskEntity {
    @TableId(type = IdType.AUTO)
    private Long id;
    private String taskUuid;
    private String type;
    private String status;       // TaskStatus.name()
    private String payload;      // JSON 字符串
    private String result;       // JSON 字符串
    private Integer subtaskTotal;
    private Integer subtaskDone;
    private Integer subtaskFailed;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
