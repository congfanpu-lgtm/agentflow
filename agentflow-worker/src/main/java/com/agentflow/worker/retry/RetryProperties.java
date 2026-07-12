package com.agentflow.worker.retry;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

/** 阶梯延迟档位(毫秒),默认 5s/30s/5m;测试用属性覆盖为短档。最多 3 次重试。 */
@Getter
@Component
public class RetryProperties {
    @Value("${agentflow.retry.delay-5s-ms:5000}")   private long delay5s;
    @Value("${agentflow.retry.delay-30s-ms:30000}") private long delay30s;
    @Value("${agentflow.retry.delay-5m-ms:300000}") private long delay5m;
    public static final int MAX_ATTEMPTS = 3;
}
