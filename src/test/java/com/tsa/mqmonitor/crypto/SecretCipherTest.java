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
        var t = cipher.encrypt("x").toCharArray();
        t[t.length - 2] ^= 1;
        assertThatThrownBy(() -> cipher.decrypt(new String(t))).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void rejectsWrongKeyLength() {
        assertThatThrownBy(() -> SecretCipher.fromBase64(Base64.getEncoder().encodeToString(new byte[16])))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("32 bytes");
    }
}
