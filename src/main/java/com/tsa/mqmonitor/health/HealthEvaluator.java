package com.tsa.mqmonitor.health;

import com.tsa.mqmonitor.mq.ChannelInfo;
import com.tsa.mqmonitor.mq.QueueInfo;
import com.tsa.mqmonitor.sampling.EvaluatedSample;
import com.tsa.mqmonitor.sampling.Sample;
import java.time.Duration;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

public class HealthEvaluator {

  public HealthVerdict queue(QueueInfo q, Thresholds t, Instant now, Instant noConsumerSince) {
    if (t.depthMax() != null && q.depth() >= t.depthMax()) {
      return HealthVerdict.unhealthy(
          HealthReason.DEPTH_THRESHOLD, "depth " + q.depth() + " reached threshold " + t.depthMax());
    }
    if (q.maxDepth() > 0) {
      int percent = (int) ((long) q.depth() * 100 / q.maxDepth());
      if (percent >= t.depthPercent()) {
        return HealthVerdict.unhealthy(
            HealthReason.DEPTH_PERCENT,
            "depth " + q.depth() + " is " + percent + "% of max " + q.maxDepth());
      }
    }
    if (q.openInputCount() != null && q.openInputCount() == 0 && noConsumerSince != null) {
      long seconds = Duration.between(noConsumerSince, now).toSeconds();
      if (seconds >= t.noConsumerSeconds()) {
        return HealthVerdict.unhealthy(HealthReason.NO_CONSUMER, "no consumer for " + seconds + "s");
      }
    }
    if (t.oldestMessageAgeSeconds() != null
        && q.oldestMessageAgeSeconds() != null
        && q.oldestMessageAgeSeconds() > t.oldestMessageAgeSeconds()) {
      return HealthVerdict.unhealthy(
          HealthReason.OLDEST_MESSAGE_AGE,
          "oldest message age "
              + q.oldestMessageAgeSeconds()
              + "s over "
              + t.oldestMessageAgeSeconds()
              + "s");
    }
    return HealthVerdict.healthy();
  }

  public HealthVerdict channel(ChannelInfo c) {
    return "RUNNING".equals(c.status())
        ? HealthVerdict.healthy()
        : HealthVerdict.unhealthy(HealthReason.CHANNEL_NOT_RUNNING, "channel status " + c.status());
  }

  public EvaluatedSample evaluate(
      Sample s, Thresholds t, Map<String, Instant> noConsumerSince, Instant now) {
    if (!s.reachable()) {
      return new EvaluatedSample(
          s,
          HealthVerdict.unhealthy(
              HealthReason.UNREACHABLE, "queue manager unreachable: " + s.error()),
          Map.of(),
          Map.of());
    }
    Map<String, HealthVerdict> queues = new LinkedHashMap<>();
    for (QueueInfo q : s.queues()) {
      queues.put(q.name(), queue(q, t, now, noConsumerSince.get(q.name())));
    }
    Map<String, HealthVerdict> channels = new LinkedHashMap<>();
    for (ChannelInfo c : s.channels()) {
      channels.put(c.name(), channel(c));
    }
    long badQueues = count(queues);
    long badChannels = count(channels);
    String reason = null;
    if (badQueues > 0) {
      reason = badQueues + (badQueues == 1 ? " queue unhealthy" : " queues unhealthy");
    }
    if (badChannels > 0) {
      String part = badChannels + (badChannels == 1 ? " channel unhealthy" : " channels unhealthy");
      reason = reason == null ? part : reason + ", " + part;
    }
    HealthVerdict connection =
        reason == null ? HealthVerdict.healthy() : HealthVerdict.unhealthy(HealthReason.OBJECTS_UNHEALTHY, reason);
    return new EvaluatedSample(s, connection, queues, channels);
  }

  private static long count(Map<String, HealthVerdict> verdicts) {
    return verdicts.values().stream().filter(v -> v.state() == HealthState.UNHEALTHY).count();
  }
}
