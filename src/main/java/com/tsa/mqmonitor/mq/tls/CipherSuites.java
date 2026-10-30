package com.tsa.mqmonitor.mq.tls;

import com.ibm.mq.jmqi.CipherSuiteUtils;

/** Maps the CipherSpec an MQ admin configures to the CipherSuite the MQ Java and JMS clients take. */
public final class CipherSuites {
  private CipherSuites() {}

  /**
   * For example {@code ANY_TLS12_OR_HIGHER} becomes {@code *TLS12ORHIGHER}. A value that is not a
   * known CipherSpec is passed through as a CipherSuite name.
   */
  public static String forCipherSpec(String cipherSpec) {
    String suite = CipherSuiteUtils.toCipherSuite(cipherSpec);
    return suite != null ? suite : cipherSpec;
  }
}
