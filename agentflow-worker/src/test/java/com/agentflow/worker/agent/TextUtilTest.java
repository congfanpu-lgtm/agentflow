package com.agentflow.worker.agent;

import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class TextUtilTest {

    @Test
    void parsesJsonWrappedInMarkdownFence() {
        String s = "好的,结果如下:\n```json\n{\"answer\":\"ok\",\"sources\":[]}\n```";
        assertEquals("ok", TextUtil.parseObject(s).get("answer").asText());
    }

    @Test
    void proseWithoutJsonThrows() {
        assertThrows(IllegalStateException.class, () -> TextUtil.parseObject("Kafka is a log."));
        assertThrows(IllegalStateException.class, () -> TextUtil.parseObject(null));
    }

    @Test
    void extractsUrlsKeepingParenthesesAndDroppingTrailingPunctuation() {
        String text = "URL: https://en.wikipedia.org/wiki/Kafka_(software).\n来源: kb://ops-1, 见 http://a.b/c;";
        assertEquals(List.of("https://en.wikipedia.org/wiki/Kafka_(software)", "kb://ops-1", "http://a.b/c"),
                TextUtil.urls(text));
    }

    @Test
    void truncatesWithEllipsis() {
        assertEquals("abc…", TextUtil.truncate("abcdef", 3));
        assertEquals("ab", TextUtil.truncate("ab", 3));
        assertEquals("", TextUtil.truncate(null, 3));
    }
}
