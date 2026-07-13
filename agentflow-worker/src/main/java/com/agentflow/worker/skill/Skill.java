package com.agentflow.worker.skill;

import java.util.Map;

/**
 * 可动态注册的技能/工具。实现类加 {@code @Component} 即被 {@link SkillRegistry} 自动收录
 * (面经"Skills 怎么动态加载":靠 Spring 收集 List&lt;Skill&gt;,零配置改动)。
 *
 * <p>输出稳定不靠提示词而靠 {@link #outputSchema()} 的代码层校验(面经"怎么保证 Skill 产出
 * 内容稳定,而不是模型自由发挥")。
 */
public interface Skill {

    String name();

    /** 多个 skill 命中同一 intent 时的排序键,数值大者优先。 */
    int priority();

    /** 该 skill 是否命中给定 intent。 */
    boolean matches(String intent);

    /** 期望输出结构:字段名 → 类型(string/number/boolean/object/array)。LLM 输出须过此校验。 */
    Map<String, String> outputSchema();

    /** 注入给模型的系统提示(引导产出符合 schema)。 */
    String systemPrompt();
}
