package com.agentflow.server.mapper;

import org.apache.ibatis.annotations.Delete;
import org.apache.ibatis.annotations.Insert;
import org.apache.ibatis.annotations.Param;

/** 推送记录:先抢占(插入成功才算轮到自己推),发送失败再释放,保证每人每件事只推一次。 */
public interface PushLogMapper {

    /** 返回 1 = 抢到;0 = 这件事已经推给过这个人(或别的实例正在推)。 */
    @Insert("INSERT IGNORE INTO push_log (user_id, event_id) VALUES (#{userId}, #{eventId})")
    int claim(@Param("userId") long userId, @Param("eventId") long eventId);

    @Delete("DELETE FROM push_log WHERE user_id = #{userId} AND event_id = #{eventId}")
    int release(@Param("userId") long userId, @Param("eventId") long eventId);
}
