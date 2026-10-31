package com.tsa.mqmonitor.service;

import static org.assertj.core.api.Assertions.assertThat;

import com.tsa.mqmonitor.enumerable.HealthReason;
import com.tsa.mqmonitor.enumerable.HealthState;
import com.tsa.mqmonitor.model.ChannelInfo;
import com.tsa.mqmonitor.model.HealthVerdict;
import com.tsa.mqmonitor.model.QueueInfo;
import com.tsa.mqmonitor.model.QueueManagerInfo;
import com.tsa.mqmonitor.model.Sample;
import com.tsa.mqmonitor.model.Thresholds;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class HealthEvaluatorTest {
  HealthEvaluator healthEvaluator = new HealthEvaluator();
  Thresholds thresholds = new Thresholds(null, 80, 60, null);
  Instant now = Instant.parse("2026-10-30T10:00:00Z");

  QueueInfo q(int depth, int max, Integer in, Integer age) {
    return new QueueInfo("APP.Q", depth, max, in, 1, age, null, null);
  }

  @Test void depthUnderPercentIsHealthy() { assertThat(healthEvaluator.queue(q(79, 100, 1, null), thresholds, now, null).state()).isEqualTo(HealthState.HEALTHY); }
  @Test void depthAtPercentIsUnhealthy() { assertThat(healthEvaluator.queue(q(80, 100, 1, null), thresholds, now, null).reason()).isEqualTo("depth 80 is 80% of max 100"); }
  @Test void absoluteDepthWins() { assertThat(healthEvaluator.queue(q(10, 5000, 1, null), thresholds.withOverrides(10, null, null, null), now, null).reason()).isEqualTo("depth 10 reached threshold 10"); }
  @Test void noConsumerWithinGraceIsHealthy() { assertThat(healthEvaluator.queue(q(1, 100, 0, null), thresholds, now, now.minusSeconds(59)).state()).isEqualTo(HealthState.HEALTHY); }
  @Test void noConsumerPastGraceIsUnhealthy() { assertThat(healthEvaluator.queue(q(1, 100, 0, null), thresholds, now, now.minusSeconds(60)).reason()).isEqualTo("no consumer for 60s"); }
  @Test void unknownOpenInputCountIsNotNoConsumer() { assertThat(healthEvaluator.queue(q(1, 100, null, null), thresholds, now, now.minusSeconds(999)).state()).isEqualTo(HealthState.HEALTHY); }
  @Test void oldestAgeThreshold() { assertThat(healthEvaluator.queue(q(1, 100, 1, 301), thresholds.withOverrides(null, null, null, 300), now, null).reason()).isEqualTo("oldest message age 301s over 300s"); }
  @Test void channelNotRunningIsUnhealthy() { assertThat(healthEvaluator.channel(new ChannelInfo("C", "SVRCONN", "STOPPED")).reason()).isEqualTo("channel status STOPPED"); }

  @Test void unreachableSampleIsUnhealthyConnection() {
    var e = healthEvaluator.evaluate(Sample.failed(1, now, "MQRC 2538"), thresholds, Map.of(), now);
    assertThat(e.connection().reason()).isEqualTo("queue manager unreachable: MQRC 2538");
  }

  @Test void anyUnhealthyQueueMakesConnectionUnhealthy() {
    var s = new Sample(1, now, new QueueManagerInfo("QM1", "RUNNING", 940, "UNIX"), List.of(q(90, 100, 1, null)), List.of(), null);
    assertThat(healthEvaluator.evaluate(s, thresholds, Map.of(), now).connection().reason()).isEqualTo("1 queue unhealthy");
  }
  @Test void codesAreAssigned() {
    assertThat(healthEvaluator.queue(q(80, 100, 1, null), thresholds, now, null).code()).isEqualTo(HealthReason.DEPTH_PERCENT);
    assertThat(healthEvaluator.queue(q(10, 5000, 1, null), thresholds.withOverrides(10, null, null, null), now, null).code()).isEqualTo(HealthReason.DEPTH_THRESHOLD);
    assertThat(healthEvaluator.queue(q(1, 100, 0, null), thresholds, now, now.minusSeconds(60)).code()).isEqualTo(HealthReason.NO_CONSUMER);
    assertThat(healthEvaluator.queue(q(1, 100, 1, 301), thresholds.withOverrides(null, null, null, 300), now, null).code()).isEqualTo(HealthReason.OLDEST_MESSAGE_AGE);
    assertThat(healthEvaluator.channel(new ChannelInfo("C", "SVRCONN", "STOPPED")).code()).isEqualTo(HealthReason.CHANNEL_NOT_RUNNING);
    assertThat(HealthVerdict.healthy().code()).isEqualTo(HealthReason.OK);
  }
}
