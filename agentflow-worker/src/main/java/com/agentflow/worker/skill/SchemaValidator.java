package com.agentflow.worker.skill;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 手写最小 JSON schema 校验器:必填字段 + 基础类型。用来把 LLM 输出约束成结构化数据,
 * 不合规则视为处理失败(交 W3 重试)——"输出稳定"是代码层校验,不是求模型听话。
 * ponytail: 完整 JSON Schema(嵌套/枚举/正则)是 backlog,当前必填+类型够演示与 enforce。
 */
@Component
public class SchemaValidator {

    /** 返回违规原因列表;空 = 通过。 */
    public List<String> validate(JsonNode json, Map<String, String> schema) {
        List<String> reasons = new ArrayList<>();
        if (json == null || !json.isObject()) {
            reasons.add("输出不是 JSON 对象");
            return reasons;
        }
        for (Map.Entry<String, String> e : schema.entrySet()) {
            String field = e.getKey();
            String type = e.getValue();
            JsonNode node = json.get(field);
            if (node == null || node.isNull()) {
                reasons.add("缺少字段:" + field);
                continue;
            }
            if (!typeOk(node, type)) {
                reasons.add("字段 " + field + " 类型应为 " + type + ",实际 " + node.getNodeType());
            }
        }
        return reasons;
    }

    private boolean typeOk(JsonNode node, String type) {
        return switch (type) {
            case "string" -> node.isTextual();
            case "number" -> node.isNumber();
            case "boolean" -> node.isBoolean();
            case "object" -> node.isObject();
            case "array" -> node.isArray();
            default -> true;   // 未知类型不拦(宽松)
        };
    }
}
