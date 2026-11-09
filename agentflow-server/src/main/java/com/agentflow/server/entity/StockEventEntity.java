package com.agentflow.server.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDate;
import java.time.LocalDateTime;

/** 某只股票的一件事:标题、摘要、重要度、出处链接、出处原文引用。 */
@Data
@TableName("stock_event")
public class StockEventEntity {
    @TableId(type = IdType.AUTO)
    private Long id;
    private String ticker;
    private LocalDate briefDate;   // 属于哪天的简报(美东日期)
    private LocalDate eventDate;   // 公告日期(仅 SEC 公告有)
    private String title;
    private String summary;
    private Integer importance;    // 1-3
    private String url;
    private String urlHash;        // sha256(url),用于唯一键
    private String sourceTitle;
    private String quote;
    private String form;           // SEC 表单类型,如 8-K
    private LocalDateTime createdAt;
}
