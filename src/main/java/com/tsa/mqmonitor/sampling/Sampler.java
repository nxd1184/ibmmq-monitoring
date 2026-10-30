package com.tsa.mqmonitor.sampling;

import com.tsa.mqmonitor.connection.MqConnection;
import com.tsa.mqmonitor.health.HealthEvaluator;
import com.tsa.mqmonitor.health.Thresholds;
import com.tsa.mqmonitor.mq.ChannelInfo;
import com.tsa.mqmonitor.mq.MqGateway;
import com.tsa.mqmonitor.mq.MqGatewayFactory;
import com.tsa.mqmonitor.mq.MqTarget;
import com.tsa.mqmonitor.mq.QueueInfo;
import com.tsa.mqmonitor.mq.QueueManagerInfo;
import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Samples one connection over a single long-lived gateway. Sampling methods are synchronized so
 * two samples of one connection never run at the same time and a gateway close never races a
 * sample. A suspended or retired sampler never touches MQ: the flags are checked under the same
 * monitor that guards the gateway, so a run that starts after suspend()/retire() cannot reopen a
 * gateway that the registry has released.
 */
public class Sampler {
  private static final Logger log = LoggerFactory.getLogger(Sampler.class);

  private final MqConnection connection;
  private final MqTarget target;
  private final Thresholds thresholds;
  private final MqGatewayFactory factory;
  private final HealthEvaluator evaluator;
  private final Clock clock;
  private final List<SampleListener> listeners;
  private final Map<String, Instant> noConsumerSince = new HashMap<>();
  private MqGateway gateway;
  private volatile EvaluatedSample latest;
  private volatile boolean suspended;
  private volatile boolean retired;

  public Sampler(
      MqConnection c,
      MqTarget target,
      Thresholds t,
      MqGatewayFactory f,
      HealthEvaluator e,
      Clock clock,
      List<SampleListener> listeners) {
    this.connection = c;
    this.target = target;
    this.thresholds = t;
    this.factory = f;
    this.evaluator = e;
    this.clock = clock;
    this.listeners = listeners;
  }

  public synchronized EvaluatedSample sampleOnce() {
    Instant now = clock.instant();
    if (!active()) {
      return inactiveResult(now);
    }
    Sample sample;
    try {
      MqGateway gw = gateway();
      QueueManagerInfo qmgr = gw.inquireQueueManager();
      List<QueueInfo> queues =
          gw.inquireQueues(connection.queueFilter(), connection.hideSystemQueues());
      List<ChannelInfo> channels = gw.inquireChannels();
      sample = new Sample(connection.id(), now, qmgr, queues, channels, null);
      trackConsumers(queues, now);
    } catch (RuntimeException e) {
      sample = failed(now, e);
    }
    return publish(evaluator.evaluate(sample, thresholds, noConsumerSince, now));
  }

  public synchronized EvaluatedSample sampleQueueManagerOnly() {
    Instant now = clock.instant();
    if (!active()) {
      return inactiveResult(now);
    }
    Sample sample;
    try {
      QueueManagerInfo qmgr = gateway().inquireQueueManager();
      sample = new Sample(connection.id(), now, qmgr, List.of(), List.of(), null);
    } catch (RuntimeException e) {
      sample = failed(now, e);
    }
    return publish(evaluator.evaluate(sample, thresholds, Map.of(), now));
  }

  public Optional<EvaluatedSample> latest() {
    return Optional.ofNullable(latest);
  }

  /** Permanently stops this sampler: closes the gateway and never opens another one. */
  public void close() {
    retire();
  }

  /** Permanent: used when the registry discards this sampler (rebuild, delete, shutdown). */
  public void retire() {
    retired = true;
    synchronized (this) {
      closeGateway();
    }
  }

  /** Blocks MQ access until resume(). Cheap and lock-free, so the registry can call it under its lock. */
  void suspend() {
    suspended = true;
  }

  void resume() {
    suspended = false;
  }

  /** Closes the gateway if the sampler is still suspended or retired; a resumed sampler keeps it. */
  public synchronized void releaseGateway() {
    if (!active()) {
      closeGateway();
    }
  }

  boolean isRetired() {
    return retired;
  }

  private boolean active() {
    return !suspended && !retired;
  }

  private EvaluatedSample inactiveResult(Instant now) {
    EvaluatedSample last = latest;
    return last != null
        ? last
        : evaluator.evaluate(
            Sample.failed(connection.id(), now, "sampler stopped"), thresholds, Map.of(), now);
  }

  private void closeGateway() {
    if (gateway != null) {
      MqGateway gw = gateway;
      gateway = null;
      try {
        gw.close();
      } catch (RuntimeException e) {
        log.debug("Closing gateway for connection {} failed: {}", connection.id(), e.toString());
      }
    }
  }

  private MqGateway gateway() {
    if (gateway == null) {
      gateway = factory.open(target);
    }
    return gateway;
  }

  private Sample failed(Instant now, RuntimeException e) {
    String message = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
    log.debug("Sample of connection {} failed: {}", connection.id(), message);
    closeGateway();
    return Sample.failed(connection.id(), now, message);
  }

  private void trackConsumers(List<QueueInfo> queues, Instant now) {
    Set<String> present = new HashSet<>();
    for (QueueInfo q : queues) {
      present.add(q.name());
      if (q.openInputCount() != null && q.openInputCount() == 0) {
        noConsumerSince.putIfAbsent(q.name(), now);
      } else {
        noConsumerSince.remove(q.name());
      }
    }
    noConsumerSince.keySet().retainAll(present);
  }

  private EvaluatedSample publish(EvaluatedSample evaluated) {
    latest = evaluated;
    for (SampleListener listener : listeners) {
      try {
        listener.onSample(evaluated);
      } catch (RuntimeException e) {
        log.warn("Sample listener failed for connection {}: {}", connection.id(), e.toString());
        log.debug("Sample listener failure detail", e);
      }
    }
    return evaluated;
  }
}
