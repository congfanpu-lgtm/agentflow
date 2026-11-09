package com.agentflow.server.mapper;

import com.agentflow.server.entity.UserEntity;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Update;

public interface UserMapper extends BaseMapper<UserEntity> {

    /** updateById 会跳过 null 字段,清空 webhook 需要显式写 NULL。 */
    @Update("UPDATE app_user SET webhook_url = #{webhookUrl}, push_hour = #{pushHour}, timezone = #{timezone} WHERE id = #{id}")
    int updateSettings(@Param("id") long id, @Param("webhookUrl") String webhookUrl,
                       @Param("pushHour") int pushHour, @Param("timezone") String timezone);
}
