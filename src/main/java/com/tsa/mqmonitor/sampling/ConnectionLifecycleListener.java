package com.tsa.mqmonitor.sampling;

/** Told when a connection stops being sampled for export: deleted, or Prometheus Export off. */
public interface ConnectionLifecycleListener {
  void connectionRemoved(long id);
}
