package com.agentflow.worker.context;

import org.springframework.stereotype.Component;

/**
 * token 数启发式估算(不引真 tokenizer)。CJK 字符约 1 字 ≈ 1 token,拉丁文约 4 字 ≈ 1 token。
 * ponytail: 启发式够限流/裁剪用;精确 tokenizer(如 jtokkit)列 backlog。系数可按实测校准。
 */
@Component
public class TokenEstimator {

    private static final double LATIN_CHARS_PER_TOKEN = 4.0;
    private static final double CJK_CHARS_PER_TOKEN = 1.5;

    public int estimate(String text) {
        if (text == null || text.isEmpty()) {
            return 0;
        }
        int cjk = 0, other = 0;
        for (int i = 0; i < text.length(); ) {
            int cp = text.codePointAt(i);
            if (isCjk(cp)) {
                cjk++;
            } else if (!Character.isWhitespace(cp)) {
                other++;
            }
            i += Character.charCount(cp);
        }
        return (int) Math.ceil(cjk / CJK_CHARS_PER_TOKEN + other / LATIN_CHARS_PER_TOKEN);
    }

    private boolean isCjk(int cp) {
        Character.UnicodeScript s = Character.UnicodeScript.of(cp);
        return s == Character.UnicodeScript.HAN
                || s == Character.UnicodeScript.HIRAGANA
                || s == Character.UnicodeScript.KATAKANA
                || s == Character.UnicodeScript.HANGUL;
    }
}
