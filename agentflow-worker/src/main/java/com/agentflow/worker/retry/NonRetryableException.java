package com.agentflow.worker.retry;

/**
 * 重试也没用的失败(输入本身不对,比如链接不合规、这一讲没有语音)。
 * {@code SubtaskListener} 收到后直接判子任务失败,不走 5s / 30s / 5m 的重试阶梯。
 */
public class NonRetryableException extends RuntimeException {
    public NonRetryableException(String message) {
        super(message);
    }
}
