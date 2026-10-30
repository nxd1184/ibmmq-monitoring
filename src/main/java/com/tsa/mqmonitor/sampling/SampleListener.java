package com.tsa.mqmonitor.sampling;

public interface SampleListener {
  void onSample(EvaluatedSample sample);

  /**
   * Called on every watchdog tick while a running connection's newest sample is older than {@link
   * SamplerRegistry#STALE_AFTER_INTERVALS} sample intervals (a hung or starved sampler). {@code
   * latest} is that old sample, unchanged.
   */
  default void onStale(EvaluatedSample latest) {}
}
