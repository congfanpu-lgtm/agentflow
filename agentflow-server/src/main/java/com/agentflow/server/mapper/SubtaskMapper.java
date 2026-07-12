package com.agentflow.server.mapper;

import com.agentflow.server.entity.SubtaskEntity;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;
import org.apache.ibatis.annotations.Update;

import java.time.LocalDateTime;
import java.util.List;

public interface SubtaskMapper extends BaseMapper<SubtaskEntity> {

    @Update("UPDATE subtask SET status = #{to} WHERE id = #{id} AND status = #{from}")
    int casStatus(@Param("id") Long id, @Param("from") String from, @Param("to") String to);

    /** 卡在 DISPATCHED 超过 cutoff 的子任务(静默失败候选)。 */
    @Select("SELECT * FROM subtask WHERE status = 'DISPATCHED' AND updated_at < #{cutoff}")
    List<SubtaskEntity> findStuckDispatched(@Param("cutoff") LocalDateTime cutoff);

    @Update("UPDATE subtask SET redispatch_count = redispatch_count + 1 WHERE id = #{id}")
    int incrementRedispatch(@Param("id") Long id);

    /** 测试专用:绕过 ON UPDATE 自动刷新,强制造出过期的 updated_at。 */
    @Update("UPDATE subtask SET updated_at = #{ts} WHERE id = #{id}")
    int setUpdatedAt(@Param("id") Long id, @Param("ts") LocalDateTime ts);
}
