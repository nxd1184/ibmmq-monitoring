package com.tsa.mqmonitor.crypto;

import javax.crypto.AEADBadTagException;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.util.Base64;

/**
 * AES-256-GCM encryption for stored secrets. Token format: base64 of {@code IV (12 bytes) || ciphertext || tag (16 bytes)}.
 */
public final class SecretCipher {

    private static final String TRANSFORMATION = "AES/GCM/NoPadding";
    private static final int KEY_BYTES = 32;
    private static final int IV_BYTES = 12;
    private static final int TAG_BITS = 128;
    private static final int TAG_BYTES = TAG_BITS / 8;
    private static final String KEY_MESSAGE = "MQMONITOR_ENCRYPTION_KEY must be base64 of 32 bytes";
    private static final String TAMPERED_MESSAGE = "ciphertext tampered or wrong key";

    private static final SecureRandom RANDOM = new SecureRandom();

    private final SecretKeySpec key;

    public SecretCipher(byte[] key32) {
        if (key32.length != KEY_BYTES) {
            throw new IllegalStateException(KEY_MESSAGE);
        }
        this.key = new SecretKeySpec(key32, "AES");
    }

    public static SecretCipher fromBase64(String key) {
        byte[] decoded;
        try {
            decoded = Base64.getDecoder().decode(key);
        } catch (IllegalArgumentException e) {
            throw new IllegalStateException(KEY_MESSAGE, e);
        }
        return new SecretCipher(decoded);
    }

    public String encrypt(String plain) {
        if (plain == null) {
            return null;
        }
        byte[] iv = new byte[IV_BYTES];
        RANDOM.nextBytes(iv);
        try {
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.ENCRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, iv));
            byte[] ciphertextWithTag = cipher.doFinal(plain.getBytes(StandardCharsets.UTF_8));
            byte[] token = ByteBuffer.allocate(IV_BYTES + ciphertextWithTag.length)
                    .put(iv)
                    .put(ciphertextWithTag)
                    .array();
            return Base64.getEncoder().encodeToString(token);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("secret encryption failed", e);
        }
    }

    public String decrypt(String token) {
        if (token == null) {
            return null;
        }
        byte[] raw = Base64.getDecoder().decode(token);
        if (raw.length < IV_BYTES + TAG_BYTES) {
            throw new IllegalArgumentException(TAMPERED_MESSAGE);
        }
        try {
            Cipher cipher = Cipher.getInstance(TRANSFORMATION);
            cipher.init(Cipher.DECRYPT_MODE, key, new GCMParameterSpec(TAG_BITS, raw, 0, IV_BYTES));
            byte[] plain = cipher.doFinal(raw, IV_BYTES, raw.length - IV_BYTES);
            return new String(plain, StandardCharsets.UTF_8);
        } catch (AEADBadTagException e) {
            throw new IllegalArgumentException(TAMPERED_MESSAGE, e);
        } catch (GeneralSecurityException e) {
            throw new IllegalStateException("secret decryption failed", e);
        }
    }
}
