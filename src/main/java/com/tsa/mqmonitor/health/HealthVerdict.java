package com.tsa.mqmonitor.health;

public record HealthVerdict(HealthState state, String reason) {

  public static HealthVerdict healthy() {
    return new HealthVerdict(HealthState.HEALTHY, "ok");
  }

  public static HealthVerdict unhealthy(String reason) {
    return new HealthVerdict(HealthState.UNHEALTHY, reason);
  }
}
