package com.agentflow.server.entity;

import com.baomidou.mybatisplus.annotation.FieldStrategy;
import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.time.LocalDateTime;

/** 一讲课。逐字稿和帧在媒体服务里(按 mediaKey 取),片段在检索服务里;这里只有状态和知识点目录。 */
@Data
@TableName("lecture")
public class LectureEntity {
    @TableId(type = IdType.AUTO)
    private Long id;
    private Long courseId;
    private Integer seq;           // 课内序号,从 1 起
    private String title;
    private String sourceUrl;      // 和 sourcePath 恰好有一个
    private String sourcePath;
    private String status;         // INGESTING / READY / PARTIAL / FAILED
    private String mediaKey;
    private Long durationMs;
    private String taskUuid;       // 最近一次摄入任务
    // 这两列要能被写回 NULL(重试成功后清掉旧的失败原因;知识点这一步失败时不留旧目录),
    // 而 updateById 默认跳过值为 null 的字段
    @TableField(updateStrategy = FieldStrategy.ALWAYS)
    private String concepts;       // JSON 字符串:[{"name","startMs","summary"}]
    @TableField(updateStrategy = FieldStrategy.ALWAYS)
    private String error;
    private LocalDateTime createdAt;
    private LocalDateTime updatedAt;
}
