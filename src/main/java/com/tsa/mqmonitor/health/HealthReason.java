package com.tsa.mqmonitor.health;

/** Bounded cause of a verdict; safe to use as a metric label. */
public enum HealthReason {
  OK,
  DEPTH_THRESHOLD,
  DEPTH_PERCENT,
  NO_CONSUMER,
  OLDEST_MESSAGE_AGE,
  CHANNEL_NOT_RUNNING,
  UNREACHABLE,
  OBJECTS_UNHEALTHY
}
