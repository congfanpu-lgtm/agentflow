package com.agentflow.worker.context;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TokenEstimatorTest {

    private final TokenEstimator est = new TokenEstimator();

    @Test
    void emptyIsZero() {
        assertEquals(0, est.estimate(null));
        assertEquals(0, est.estimate(""));
    }

    @Test
    void latinAboutFourCharsPerToken() {
        // 40 个非空白拉丁字符 ≈ 10 token
        int t = est.estimate("abcdefghij abcdefghij abcdefghij abcdefghij");
        assertTrue(t >= 8 && t <= 12, "got " + t);
    }

    @Test
    void cjkDenserThanLatin() {
        // 同字符数下,CJK 估算的 token 应多于拉丁
        assertTrue(est.estimate("中文中文中文中文") > est.estimate("abcdabcd"));
    }
}
