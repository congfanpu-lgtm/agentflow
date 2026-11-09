package com.agentflow.server.mapper;

import com.agentflow.server.entity.StockEventEntity;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Insert;

public interface StockEventMapper extends BaseMapper<StockEventEntity> {

    /** 同一只股票的同一个出处已存在时什么都不做(返回 0),所以任务重跑不会产生重复事件。 */
    @Insert("INSERT IGNORE INTO stock_event (ticker, brief_date, event_date, title, summary, importance, url, url_hash, "
            + "source_title, quote, form) VALUES (#{ticker}, #{briefDate}, #{eventDate}, #{title}, #{summary}, "
            + "#{importance}, #{url}, #{urlHash}, #{sourceTitle}, #{quote}, #{form})")
    int insertIgnore(StockEventEntity e);
}
