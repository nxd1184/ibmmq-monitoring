package com.tsa.mqmonitor.crypto;

import org.junit.jupiter.api.Test;

import java.util.Base64;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SecretCipherTest {

    SecretCipher cipher = new SecretCipher(new byte[32]);

    @Test
    void roundTrips() {
        assertThat(cipher.decrypt(cipher.encrypt("s3cret"))).isEqualTo("s3cret");
    }

    @Test
    void sameInputDifferentTokens() {
        assertThat(cipher.encrypt("x")).isNotEqualTo(cipher.encrypt("x"));
    }

    @Test
    void nullPassesThrough() {
        assertThat(cipher.encrypt(null)).isNull();
        assertThat(cipher.decrypt(null)).isNull();
    }

    @Test
    void tamperedTokenFails() {
        byte[] raw = Base64.getDecoder().decode(cipher.encrypt("x"));
        raw[raw.length - 1] ^= 1;
        String tampered = Base64.getEncoder().encodeToString(raw);
        assertThatThrownBy(() -> cipher.decrypt(tampered)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nonBase64TokenFails() {
        assertThatThrownBy(() -> cipher.decrypt("not base64!")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shortTokenFails() {
        String shortToken = Base64.getEncoder().encodeToString(new byte[12 + 16 - 1]);
        assertThatThrownBy(() -> cipher.decrypt(shortToken)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsWrongKeyLength() {
        assertThatThrownBy(() -> SecretCipher.fromBase64(Base64.getEncoder().encodeToString(new byte[16])))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("32 bytes");
    }

    @Test
    void constructorRejectsWrongKeyLength() {
        assertThatThrownBy(() -> new SecretCipher(new byte[16]))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("MQMONITOR_ENCRYPTION_KEY must be base64 of 32 bytes");
    }
}
