package com.tsa.mqmonitor.service;

import com.tsa.mqmonitor.client.MqGateway;
import com.tsa.mqmonitor.client.MqGatewayFactory;
import com.tsa.mqmonitor.model.ChannelInfo;
import com.tsa.mqmonitor.model.EvaluatedSample;
import com.tsa.mqmonitor.model.MqConnection;
import com.tsa.mqmonitor.model.MqTarget;
import com.tsa.mqmonitor.model.QueueInfo;
import com.tsa.mqmonitor.model.QueueManagerInfo;
import com.tsa.mqmonitor.model.Sample;
import com.tsa.mqmonitor.model.Thresholds;
import com.tsa.mqmonitor.util.BoundedCall;
import java.time.Clock;
import java.time.Instant;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Samples one connection over a single long-lived gateway.
 *
 * <p>No lock is held across MQ I/O by anything but the sampling path itself: {@code sampleLock}
 * serializes the samples of this connection (each bounded by the gateway's per-sample deadline),
 * while {@link #retire()}, {@link #releaseGateway()} and {@link #close()} only swap the gateway
 * reference and close the old gateway on a daemon thread, so they return at once even while a
 * sample is blocked on a frozen queue manager. A sample that is running when its sampler is
 * retired loses its gateway, and its result is discarded rather than published. A suspended or
 * retired sampler never opens a gateway: a gateway opened concurrently with suspend()/retire() is
 * closed again as soon as the flag is seen.
 */
public class Sampler {
  private static final Logger log = LoggerFactory.getLogger(Sampler.class);

  private final MqConnection mqConnection;
  private final MqTarget mqTarget;
  private final Thresholds thresholds;
  private final MqGatewayFactory mqGatewayFactory;
  private final HealthEvaluator healthEvaluator;
  private final Clock clock;
  private final List<SampleListener> sampleListeners;
  private final ReentrantLock sampleLock = new ReentrantLock();
  /** Guarded by {@code sampleLock}. */
  private final Map<String, Instant> noConsumerSince = new HashMap<>();
  private final AtomicReference<MqGateway> gateway = new AtomicReference<>();
  private volatile EvaluatedSample latest;
  private volatile boolean suspended;
  private volatile boolean retired;

  public Sampler(
      MqConnection mqConnection,
      MqTarget mqTarget,
      Thresholds thresholds,
      MqGatewayFactory mqGatewayFactory,
      HealthEvaluator healthEvaluator,
      Clock clock,
      List<SampleListener> sampleListeners) {
    this.mqConnection = mqConnection;
    this.mqTarget = mqTarget;
    this.thresholds = thresholds;
    this.mqGatewayFactory = mqGatewayFactory;
    this.healthEvaluator = healthEvaluator;
    this.clock = clock;
    this.sampleListeners = sampleListeners;
  }

  public EvaluatedSample sampleOnce() {
    EvaluatedSample evaluated;
    sampleLock.lock();
    try {
      Instant now = clock.instant();
      if (!active()) {
        return inactiveResult(now);
      }
      Sample sample;
      MqGateway gw = null;
      try {
        gw = gateway();
        gw.beginSample();
        QueueManagerInfo qmgr = gw.inquireQueueManager();
        List<QueueInfo> queues =
            gw.inquireQueues(mqConnection.queueFilter(), mqConnection.hideSystemQueues());
        List<ChannelInfo> channels = gw.inquireChannels();
        sample = new Sample(mqConnection.id(), now, qmgr, queues, channels, null);
        trackConsumers(queues, now);
      } catch (RuntimeException e) {
        if (!active()) {
          // stopped while sampling: the failure is ours (gateway closed under it), not MQ's
          closeIfCurrent(gw);
          return inactiveResult(now);
        }
        sample = failed(now, gw, e);
      }
      evaluated = healthEvaluator.evaluate(sample, thresholds, noConsumerSince, now);
    } finally {
      sampleLock.unlock();
    }
    return publish(evaluated);
  }

  public EvaluatedSample sampleQueueManagerOnly() {
    EvaluatedSample evaluated;
    sampleLock.lock();
    try {
      Instant now = clock.instant();
      if (!active()) {
        return inactiveResult(now);
      }
      Sample sample;
      MqGateway gw = null;
      try {
        gw = gateway();
        gw.beginSample();
        QueueManagerInfo qmgr = gw.inquireQueueManager();
        sample = new Sample(mqConnection.id(), now, qmgr, List.of(), List.of(), null);
      } catch (RuntimeException e) {
        if (!active()) {
          // stopped while sampling: the failure is ours (gateway closed under it), not MQ's
          closeIfCurrent(gw);
          return inactiveResult(now);
        }
        sample = failed(now, gw, e);
      }
      evaluated = healthEvaluator.evaluate(sample, thresholds, Map.of(), now);
    } finally {
      sampleLock.unlock();
    }
    return publish(evaluated);
  }

  public Optional<EvaluatedSample> latest() {
    return Optional.ofNullable(latest);
  }

  /** Permanently stops this sampler: closes the gateway and never opens another one. */
  public void close() {
    retire();
  }

  /**
   * Permanent: used when the registry discards this sampler (rebuild, delete, shutdown). Returns
   * at once; the gateway is closed on a daemon thread, which also unblocks a running sample.
   */
  public void retire() {
    retired = true;
    closeGateway(gateway.getAndSet(null));
  }

  /** Blocks MQ access until resume(). Cheap and lock-free, so the registry can call it under its lock. */
  void suspend() {
    suspended = true;
  }

  void resume() {
    suspended = false;
  }

  /** Closes the gateway if the sampler is still suspended or retired; a resumed sampler keeps it. */
  public void releaseGateway() {
    if (!active()) {
      closeGateway(gateway.getAndSet(null));
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
        : healthEvaluator.evaluate(
            Sample.failed(mqConnection.id(), now, "sampler stopped"), thresholds, Map.of(), now);
  }

  /** Closes without blocking the caller: a close on a dead connection can hang on its socket. */
  private void closeGateway(MqGateway gw) {
    if (gw != null) {
      long id = mqConnection.id();
      BoundedCall.runAsync("closing gateway for connection " + id, gw::close);
    }
  }

  private MqGateway gateway() {
    MqGateway gw = gateway.get();
    if (gw != null) {
      return gw;
    }
    gw = mqGatewayFactory.open(mqTarget);
    if (!gateway.compareAndSet(null, gw)) {
      closeGateway(gw); // cannot happen while samples are serialized; never leak a mqConnection
      throw new IllegalStateException("gateway replaced concurrently");
    }
    if (!active()) {
      // retire()/suspend() raced the open: whichever ran second sees the other and closes it.
      closeGateway(gateway.getAndSet(null));
      throw new IllegalStateException("sampler stopped");
    }
    return gw;
  }

  private Sample failed(Instant now, MqGateway gw, RuntimeException e) {
    String message = e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName();
    log.debug("Sample of connection {} failed: {}", mqConnection.id(), message);
    closeIfCurrent(gw);
    return Sample.failed(mqConnection.id(), now, message);
  }

  /** Drops a failed gateway so the next sample reconnects; leaves a newer gateway alone. */
  private void closeIfCurrent(MqGateway gw) {
    if (gw != null && gateway.compareAndSet(gw, null)) {
      closeGateway(gw);
    }
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

  /** Runs outside every lock. A retired sampler publishes nothing, not even a late result. */
  private EvaluatedSample publish(EvaluatedSample evaluated) {
    if (retired) {
      return evaluated;
    }
    logReachabilityChange(latest, evaluated);
    latest = evaluated;
    for (SampleListener listener : sampleListeners) {
      try {
        listener.onSample(evaluated);
      } catch (RuntimeException e) {
        log.warn("Sample listener failed for connection {}: {}", mqConnection.id(), e.toString());
        log.debug("Sample listener failure detail", e);
      }
    }
    return evaluated;
  }

  /** One WARN when a connection stops answering and one INFO when it answers again. */
  private void logReachabilityChange(EvaluatedSample previous, EvaluatedSample next) {
    boolean wasReachable = previous == null || previous.sample().reachable();
    boolean isReachable = next.sample().reachable();
    if (wasReachable && !isReachable) {
      log.warn("Connection {} ({}) is unreachable: {}",
          mqConnection.id(), mqConnection.name(), next.sample().error());
    } else if (!wasReachable && isReachable) {
      log.info("Connection {} ({}) is reachable again", mqConnection.id(), mqConnection.name());
    }
  }
}
