package com.tsa.mqmonitor.health;

import static org.assertj.core.api.Assertions.assertThat;

import com.tsa.mqmonitor.mq.ChannelInfo;
import com.tsa.mqmonitor.mq.QueueInfo;
import com.tsa.mqmonitor.mq.QueueManagerInfo;
import com.tsa.mqmonitor.sampling.Sample;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class HealthEvaluatorTest {
  HealthEvaluator eval = new HealthEvaluator();
  Thresholds t = new Thresholds(null, 80, 60, null);
  Instant now = Instant.parse("2026-10-30T10:00:00Z");

  QueueInfo q(int depth, int max, Integer in, Integer age) {
    return new QueueInfo("APP.Q", depth, max, in, 1, age, null, null);
  }

  @Test void depthUnderPercentIsHealthy() { assertThat(eval.queue(q(79, 100, 1, null), t, now, null).state()).isEqualTo(HealthState.HEALTHY); }
  @Test void depthAtPercentIsUnhealthy() { assertThat(eval.queue(q(80, 100, 1, null), t, now, null).reason()).isEqualTo("depth 80 is 80% of max 100"); }
  @Test void absoluteDepthWins() { assertThat(eval.queue(q(10, 5000, 1, null), t.withOverrides(10, null, null, null), now, null).reason()).isEqualTo("depth 10 reached threshold 10"); }
  @Test void noConsumerWithinGraceIsHealthy() { assertThat(eval.queue(q(1, 100, 0, null), t, now, now.minusSeconds(59)).state()).isEqualTo(HealthState.HEALTHY); }
  @Test void noConsumerPastGraceIsUnhealthy() { assertThat(eval.queue(q(1, 100, 0, null), t, now, now.minusSeconds(60)).reason()).isEqualTo("no consumer for 60s"); }
  @Test void unknownOpenInputCountIsNotNoConsumer() { assertThat(eval.queue(q(1, 100, null, null), t, now, now.minusSeconds(999)).state()).isEqualTo(HealthState.HEALTHY); }
  @Test void oldestAgeThreshold() { assertThat(eval.queue(q(1, 100, 1, 301), t.withOverrides(null, null, null, 300), now, null).reason()).isEqualTo("oldest message age 301s over 300s"); }
  @Test void channelNotRunningIsUnhealthy() { assertThat(eval.channel(new ChannelInfo("C", "SVRCONN", "STOPPED")).reason()).isEqualTo("channel status STOPPED"); }

  @Test void unreachableSampleIsUnhealthyConnection() {
    var e = eval.evaluate(Sample.failed(1, now, "MQRC 2538"), t, Map.of(), now);
    assertThat(e.connection().reason()).isEqualTo("queue manager unreachable: MQRC 2538");
  }

  @Test void anyUnhealthyQueueMakesConnectionUnhealthy() {
    var s = new Sample(1, now, new QueueManagerInfo("QM1", "RUNNING", 940, "UNIX"), List.of(q(90, 100, 1, null)), List.of(), null);
    assertThat(eval.evaluate(s, t, Map.of(), now).connection().reason()).isEqualTo("1 queue unhealthy");
  }
  @Test void codesAreAssigned() {
    assertThat(eval.queue(q(80, 100, 1, null), t, now, null).code()).isEqualTo(HealthReason.DEPTH_PERCENT);
    assertThat(eval.queue(q(10, 5000, 1, null), t.withOverrides(10, null, null, null), now, null).code()).isEqualTo(HealthReason.DEPTH_THRESHOLD);
    assertThat(eval.queue(q(1, 100, 0, null), t, now, now.minusSeconds(60)).code()).isEqualTo(HealthReason.NO_CONSUMER);
    assertThat(eval.queue(q(1, 100, 1, 301), t.withOverrides(null, null, null, 300), now, null).code()).isEqualTo(HealthReason.OLDEST_MESSAGE_AGE);
    assertThat(eval.channel(new ChannelInfo("C", "SVRCONN", "STOPPED")).code()).isEqualTo(HealthReason.CHANNEL_NOT_RUNNING);
    assertThat(HealthVerdict.healthy().code()).isEqualTo(HealthReason.OK);
  }
}
