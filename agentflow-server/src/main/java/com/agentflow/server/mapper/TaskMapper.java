package com.agentflow.server.mapper;

import com.agentflow.server.entity.TaskEntity;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

public interface TaskMapper extends BaseMapper<TaskEntity> {

    /** 数据库 CAS:仅当当前状态为 from 时迁移到 to,返回影响行数(0=竞争失败)。 */
    @Update("UPDATE task SET status = #{to} WHERE id = #{id} AND status = #{from}")
    int casStatus(@Param("id") Long id, @Param("from") String from, @Param("to") String to);

    /** 原子进度累加,避免并发丢更新与 COUNT 扫描。 */
    @Update("UPDATE task SET subtask_done = subtask_done + 1 WHERE id = #{id}")
    int incrementDone(@Param("id") Long id);

    /** 失败计数原子累加(部分失败语义:done+failed==total 时判终态)。 */
    @Update("UPDATE task SET subtask_failed = subtask_failed + 1 WHERE id = #{id}")
    int incrementFailed(@Param("id") Long id);

    /** DLQ 恢复:失败计数原子回退(子任务由 FAILED 重置为 PENDING 时对应扣减)。 */
    @Update("UPDATE task SET subtask_failed = subtask_failed - 1 WHERE id = #{id}")
    int decrementFailed(@Param("id") Long id);
}
