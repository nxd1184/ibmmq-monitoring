package com.tsa.mqmonitor.service.impl;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class CipherSuitesTest {
  @Test
  void cipherSpecIsTranslatedToCipherSuite() {
    assertThat(CipherSuites.forCipherSpec("ANY_TLS12_OR_HIGHER")).isEqualTo("*TLS12ORHIGHER");
    assertThat(CipherSuites.forCipherSpec("ECDHE_RSA_AES_128_GCM_SHA256"))
        .isEqualTo("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
    assertThat(CipherSuites.forCipherSpec("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"))
        .isEqualTo("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");
  }
}
