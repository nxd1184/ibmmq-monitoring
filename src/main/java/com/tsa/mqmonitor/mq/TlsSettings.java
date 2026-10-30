package com.tsa.mqmonitor.mq;

import com.tsa.mqmonitor.connection.TlsMode;

public record TlsSettings(
    TlsMode mode,
    String cipherSpec,
    String truststorePath,
    String truststorePassword,
    String keystorePath,
    String keystorePassword,
    String certificateLabel) {

  /** Never prints the store passwords. */
  @Override
  public String toString() {
    return "TlsSettings[mode=" + mode + ", cipherSpec=" + cipherSpec + ", truststorePath="
        + truststorePath + ", truststorePassword=" + Redact.secret(truststorePassword)
        + ", keystorePath=" + keystorePath + ", keystorePassword=" + Redact.secret(keystorePassword)
        + ", certificateLabel=" + certificateLabel + "]";
  }

  public static TlsSettings none() {
    return new TlsSettings(TlsMode.NONE, null, null, null, null, null, null);
  }
}
