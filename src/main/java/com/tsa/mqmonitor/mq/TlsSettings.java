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

  public static TlsSettings none() {
    return new TlsSettings(TlsMode.NONE, null, null, null, null, null, null);
  }
}
