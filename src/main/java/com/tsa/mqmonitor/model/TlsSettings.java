package com.tsa.mqmonitor.model;

import com.tsa.mqmonitor.enumerable.TlsMode;
import com.tsa.mqmonitor.util.Redact;

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
