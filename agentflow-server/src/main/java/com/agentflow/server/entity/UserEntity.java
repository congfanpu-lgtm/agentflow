package com.agentflow.server.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("app_user")
public class UserEntity {
    @TableId(type = IdType.AUTO)
    private Long id;
    private String email;
    private String passwordHash;
    private String webhookUrl;   // 飞书 / Slack 机器人地址;从不原样返回给前端
    private Integer pushHour;    // 0-23,按 timezone 的本地小时
    private String timezone;
    private LocalDateTime createdAt;
}
