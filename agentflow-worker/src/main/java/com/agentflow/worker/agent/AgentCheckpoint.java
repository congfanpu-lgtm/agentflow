package com.agentflow.worker.agent;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RList;
import org.redisson.api.RedissonClient;
import org.redisson.client.codec.StringCodec;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

/**
 * agent 续跑存档(Redis list,key = agent:ckpt:{subtaskUuid},每步一条 JSON,TTL 24h)。
 * 它只是"续跑缓存",不是业务状态:子任务成败仍只由 server 在 MySQL 里 CAS 落定,worker 不连 MySQL。
 * 丢了最近一步的存档 = 该步重做一次;工具只读,重做无副作用。
 * ponytail: 超时重投与原 worker 同时存活时可能双跑,load 按步号去重兜底;需要严格互斥再加 Redis 锁。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class AgentCheckpoint {

    static final String PREFIX = "agent:ckpt:";
    static final Duration TTL = Duration.ofHours(24);
    private static final ObjectMapper OM = new ObjectMapper();

    private final RedissonClient redisson;

    /** 返回从第 1 步开始连续的已完成步骤;存档损坏则清空、从头开始。 */
    public List<AgentStep> load(String subtaskUuid) {
        RList<String> list = list(subtaskUuid);
        List<AgentStep> steps = new ArrayList<>();
        try {
            for (String json : list.readAll()) {
                AgentStep s = OM.readValue(json, AgentStep.class);
                if (s.step() == steps.size() + 1) {
                    steps.add(s);
                }
            }
        } catch (Exception e) {
            log.warn("agent 存档损坏,丢弃并从头开始 subtaskUuid={}", subtaskUuid, e);
            list.delete();
            return List.of();
        }
        return steps;
    }

    public void append(String subtaskUuid, AgentStep step) {
        RList<String> list = list(subtaskUuid);
        try {
            list.add(OM.writeValueAsString(step));
        } catch (Exception e) {
            throw new IllegalStateException("agent 存档序列化失败", e);
        }
        list.expire(TTL);
    }

    private RList<String> list(String subtaskUuid) {
        return redisson.getList(PREFIX + subtaskUuid, StringCodec.INSTANCE);
    }
}
