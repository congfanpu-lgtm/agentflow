package com.agentflow.server.mapper;

import com.agentflow.server.entity.SubtaskEntity;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

public interface SubtaskMapper extends BaseMapper<SubtaskEntity> {

    @Update("UPDATE subtask SET status = #{to} WHERE id = #{id} AND status = #{from}")
    int casStatus(@Param("id") Long id, @Param("from") String from, @Param("to") String to);
}
