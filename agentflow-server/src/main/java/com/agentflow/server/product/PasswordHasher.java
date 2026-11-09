package com.agentflow.server.product;

import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.PBEKeySpec;
import java.security.GeneralSecurityException;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * 密码哈希:JDK 自带的 PBKDF2-HMAC-SHA256,60 万次迭代,每个密码一个随机盐。
 * 存储格式 {@code pbkdf2$<迭代次数>$<盐 base64>$<哈希 base64>},迭代次数跟着哈希走,以后调大不影响旧密码。
 */
public final class PasswordHasher {

    private static final int ITERATIONS = 600_000;
    private static final SecureRandom RANDOM = new SecureRandom();

    private PasswordHasher() {
    }

    public static String hash(String password) {
        byte[] salt = new byte[16];
        RANDOM.nextBytes(salt);
        Base64.Encoder b64 = Base64.getEncoder();
        return "pbkdf2$" + ITERATIONS + "$" + b64.encodeToString(salt) + "$"
                + b64.encodeToString(derive(password, salt, ITERATIONS));
    }

    /** 任何格式不对的存储串都返回 false,不抛异常。 */
    public static boolean verify(String password, String stored) {
        try {
            String[] p = stored.split("\\$");
            if (p.length != 4 || !"pbkdf2".equals(p[0])) {
                return false;
            }
            byte[] expected = Base64.getDecoder().decode(p[3]);
            byte[] actual = derive(password, Base64.getDecoder().decode(p[2]), Integer.parseInt(p[1]));
            return MessageDigest.isEqual(expected, actual);   // 恒定时间比较
        } catch (RuntimeException e) {
            return false;
        }
    }

    private static byte[] derive(String password, byte[] salt, int iterations) {
        try {
            return SecretKeyFactory.getInstance("PBKDF2WithHmacSHA256")
                    .generateSecret(new PBEKeySpec(password.toCharArray(), salt, iterations, 256)).getEncoded();
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException(e);
        }
    }
}
