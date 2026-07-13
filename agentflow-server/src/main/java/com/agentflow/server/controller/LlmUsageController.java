package com.agentflow.server.controller;

import org.redisson.api.RedissonClient;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

/**
 * LLM token 记账查询(简历指标3/6 数据来源)。直接读 worker 记账写入的 Redis 计数器,
 * key 规约见 {@code TokenAccountant}:{@code agentflow:tokens:<model>:{prompt,completion,costmicro}}。
 * server 与 worker 不同模块、不共享代码,靠这组约定的 key 解耦(server 不需要 LLM 配置)。
 */
@RestController
@RequestMapping("/api/v1/llm")
@RequiredArgsConstructor
public class LlmUsageController {

    private static final String PREFIX = "agentflow:tokens:";

    private final RedissonClient redisson;

    public record UsageView(String model, long promptTokens, long completionTokens,
                            long totalTokens, double costUsd) {}

    @GetMapping("/usage")
    public List<UsageView> usage() {
        List<UsageView> out = new ArrayList<>();
        for (String promptKey : redisson.getKeys().getKeysByPattern(PREFIX + "*:prompt")) {
            String base = promptKey.substring(0, promptKey.length() - ":prompt".length());
            String model = base.substring(PREFIX.length());
            long prompt = redisson.getAtomicLong(base + ":prompt").get();
            long completion = redisson.getAtomicLong(base + ":completion").get();
            long costMicro = redisson.getAtomicLong(base + ":costmicro").get();
            out.add(new UsageView(model, prompt, completion, prompt + completion, costMicro / 1_000_000.0));
        }
        return out;
    }
}
