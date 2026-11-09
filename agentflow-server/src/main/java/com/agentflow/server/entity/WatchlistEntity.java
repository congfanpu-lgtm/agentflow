package com.agentflow.server.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

@Data
@TableName("watchlist")
public class WatchlistEntity {
    @TableId(type = IdType.AUTO)
    private Long id;
    private Long userId;
    private String ticker;
    private String companyName;
    private LocalDateTime createdAt;
}
