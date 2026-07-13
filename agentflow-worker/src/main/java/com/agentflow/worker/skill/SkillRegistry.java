package com.agentflow.worker.skill;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.Comparator;
import java.util.List;
import java.util.Optional;

/**
 * 技能注册表:Spring 把所有 {@link Skill} 实现注入 {@code List<Skill>},新增技能只需 @Component
 * 即自动纳入(动态注册)。多命中按 priority 降序、name 稳定排序(面经"多个 skill 命中怎么排序")。
 */
@Component
@RequiredArgsConstructor
public class SkillRegistry {

    private final List<Skill> skills;

    /** 命中 intent 的技能,按优先级降序、name 升序稳定排序。 */
    public List<Skill> select(String intent) {
        return skills.stream()
                .filter(s -> s.matches(intent))
                .sorted(Comparator.comparingInt(Skill::priority).reversed()
                        .thenComparing(Skill::name))
                .toList();
    }

    /** 命中优先级最高的技能。 */
    public Optional<Skill> selectBest(String intent) {
        return select(intent).stream().findFirst();
    }
}
