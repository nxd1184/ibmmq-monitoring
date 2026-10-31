package com.tsa.mqmonitor.security;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Base64;
import org.junit.jupiter.api.Test;

class SecretCipherTest {

    SecretCipher secretCipher = new SecretCipher(new byte[32]);

    @Test
    void roundTrips() {
        assertThat(secretCipher.decrypt(secretCipher.encrypt("s3cret"))).isEqualTo("s3cret");
    }

    @Test
    void sameInputDifferentTokens() {
        assertThat(secretCipher.encrypt("x")).isNotEqualTo(secretCipher.encrypt("x"));
    }

    @Test
    void nullPassesThrough() {
        assertThat(secretCipher.encrypt(null)).isNull();
        assertThat(secretCipher.decrypt(null)).isNull();
    }

    @Test
    void tamperedTokenFails() {
        byte[] raw = Base64.getDecoder().decode(secretCipher.encrypt("x"));
        raw[raw.length - 1] ^= 1;
        String tampered = Base64.getEncoder().encodeToString(raw);
        assertThatThrownBy(() -> secretCipher.decrypt(tampered)).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void nonBase64TokenFails() {
        assertThatThrownBy(() -> secretCipher.decrypt("not base64!")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void shortTokenFails() {
        String shortToken = Base64.getEncoder().encodeToString(new byte[12 + 16 - 1]);
        assertThatThrownBy(() -> secretCipher.decrypt(shortToken)).isInstanceOf(IllegalArgumentException.class);
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
