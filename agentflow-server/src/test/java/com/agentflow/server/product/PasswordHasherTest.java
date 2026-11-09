package com.agentflow.server.product;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class PasswordHasherTest {

    @Test
    void sameHashTwiceDiffersButBothVerify() {
        String a = PasswordHasher.hash("correct horse");
        String b = PasswordHasher.hash("correct horse");
        assertNotEquals(a, b, "每次用新的随机盐");
        assertTrue(a.startsWith("pbkdf2$600000$"));
        assertTrue(PasswordHasher.verify("correct horse", a));
        assertTrue(PasswordHasher.verify("correct horse", b));
    }

    @Test
    void wrongPasswordOrBrokenHashIsRejected() {
        String h = PasswordHasher.hash("correct horse");
        assertFalse(PasswordHasher.verify("wrong horse", h));
        assertFalse(PasswordHasher.verify("correct horse", h.substring(0, h.length() - 4) + "AAAA"));
        assertFalse(PasswordHasher.verify("correct horse", "not-a-hash"));
        assertFalse(PasswordHasher.verify("correct horse", "pbkdf2$abc$x$y"));
        assertFalse(PasswordHasher.verify("correct horse", null));
    }
}
