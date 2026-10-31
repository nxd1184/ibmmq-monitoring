package com.tsa.mqmonitor.model;

public record Thresholds(
    Integer depthMax, int depthPercent, int noConsumerSeconds, Integer oldestMessageAgeSeconds) {

  public Thresholds withOverrides(
      Integer depthMax, Integer depthPercent, Integer noConsumerSeconds, Integer oldestAge) {
    return new Thresholds(
        depthMax != null ? depthMax : this.depthMax,
        depthPercent != null ? depthPercent : this.depthPercent,
        noConsumerSeconds != null ? noConsumerSeconds : this.noConsumerSeconds,
        oldestAge != null ? oldestAge : this.oldestMessageAgeSeconds);
  }
}
