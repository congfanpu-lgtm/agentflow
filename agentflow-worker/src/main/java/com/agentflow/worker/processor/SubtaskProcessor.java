package com.agentflow.worker.processor;

import com.agentflow.common.mq.SubtaskMessage;

/**
 * 子任务处理器(worker 内 Agent 执行面 = Runtime)。每种任务类型一个实现,
 * {@code SubtaskListener} 按 {@code SubtaskMessage.type} 路由到对应处理器(策略模式,Spring 自动收集)。
 * 修 backlog T4b:处理器不再硬编码单一类型。
 */
public interface SubtaskProcessor {
    /** 处理的任务类型,与 {@code SubtaskMessage.type} / decomposer.type() 对齐。 */
    String type();

    /** 处理输入 JSON,返回输出 JSON。抛异常 → 交 W3 重试路由。 */
    String process(String inputJson) throws Exception;

    /** 需要子任务身份(如按 subtaskUuid 存档续跑)的处理器覆盖此方法;默认只看 input。 */
    default String process(SubtaskMessage msg) throws Exception {
        return process(msg.getInputJson());
    }
}
