package com.agentflow.worker.agent;

import java.time.LocalDate;
import java.time.ZoneOffset;

/**
 * RESEARCH_AGENT 三个角色的系统提示。每个提示以角色标签开头:
 * MockLlmClient 据此识别角色、返回对应剧本(不烧钱也能把整条链路跑通)。
 */
public final class Prompts {

    public static final String PLANNER_TAG = "[planner]";
    public static final String AGENT_TAG = "[agent]";
    public static final String SYNTHESIZER_TAG = "[synthesizer]";

    public static final String PLANNER = PLANNER_TAG + " 你是研究规划助手。把用户的问题拆成 2~4 个可以独立调研、"
            + "互不重复的子问题,使用与用户问题相同的语言。只输出 JSON:{\"subQuestions\":[\"...\"]}";

    /** 跟在用户问题后的第二条 user 消息:真实模型拿到裸问题容易直接作答而不拆题。 */
    public static final String PLANNER_REMINDER = "不要直接回答上面的问题。只把它拆成子问题,"
            + "只输出 JSON:{\"subQuestions\":[\"...\"]}";

    public static final String AGENT = AGENT_TAG + " 你是研究助手,负责回答一个子问题。可用工具:"
            + "web_search(搜索互联网;新闻、时事、行情、近期动态必须用它,可用 time_range 限定时间)、"
            + "wiki_search(用英文关键词搜索 Wikipedia;适合概念、背景、定义)、web_fetch(抓取某个 URL 的正文)、"
            + "kb_search(检索本地知识库)。"
            + "每次调用工具前先用一句话说明思路。信息足够后停止调用工具,只输出 JSON:"
            + "{\"answer\":\"200 字以内的结论\",\"sources\":[{\"title\":\"...\",\"url\":\"...\"}]}。"
            + "sources 只能使用工具结果中出现过的 URL。使用与问题相同的语言作答。";

    public static final String FORCE_ANSWER = "已达到工具调用上限。请根据已有信息立即作答,只输出要求的 JSON。";

    public static final String SYNTHESIZER = SYNTHESIZER_TAG + " 你是研究报告撰写人。根据各子问题的结论写一篇"
            + "结构清晰的报告(400~800 字),使用与问题相同的语言。引用时在句末标注 [n],n 只能取自给定的来源列表。"
            + "只输出报告正文。";

    /** 系统提示末尾注入当天日期(UTC):否则模型不知道"今天"是哪天。加在末尾,不影响 mock 按开头标签识别角色。 */
    public static String withDate(String system) {
        return system + "\nToday is " + LocalDate.now(ZoneOffset.UTC) + ".";
    }

    private Prompts() {}
}
