package com.agentflow.server.policy;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBucket;
import org.redisson.api.RedissonClient;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * 统一副作用出口 + 业务级幂等:所有副作用经 execute 执行,businessKey 去重。
 * 代码层 enforce("只能经这里"),防同一业务动作被触发多次(如重复发通知/建 case)。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SideEffectPolicy {

    private static final Duration TTL = Duration.ofHours(24);
    private static final String PREFIX = "agentflow:sideeffect:";

    private final RedissonClient redisson;

    /** 首次:执行 action + 占键,返回 true;重复:跳过,返回 false。 */
    public boolean execute(String businessKey, Runnable action) {
        RBucket<String> bucket = redisson.getBucket(PREFIX + businessKey);
        if (!bucket.setIfAbsent("1", TTL)) {
            log.info("副作用去重跳过 businessKey={}", businessKey);
            return false;
        }
        try {
            action.run();
            return true;
        } catch (RuntimeException e) {
            // 执行失败:释放占位键,让后续重试可再执行(避免"标记已完成但实际没做")
            bucket.delete();
            throw e;
        }
    }
}
