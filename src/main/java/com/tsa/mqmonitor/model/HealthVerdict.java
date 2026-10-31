package com.tsa.mqmonitor.model;

public record HealthVerdict(HealthState state, HealthReason code, String reason) {

  public static HealthVerdict healthy() {
    return new HealthVerdict(HealthState.HEALTHY, HealthReason.OK, "ok");
  }

  public static HealthVerdict unhealthy(HealthReason code, String reason) {
    return new HealthVerdict(HealthState.UNHEALTHY, code, reason);
  }
}
