package com.tsa.mqmonitor.mq;

/** Masks secrets in {@code toString()} output. */
public final class Redact {
  private Redact() {}

  /** {@code ***} for a set secret, {@code null} for an absent one. */
  public static String secret(String value) {
    return value == null ? null : "***";
  }
}
