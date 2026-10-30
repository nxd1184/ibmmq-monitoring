package com.tsa.mqmonitor.sampling;

import com.tsa.mqmonitor.config.MqMonitorProperties;
import com.tsa.mqmonitor.connection.ConnectionService;
import com.tsa.mqmonitor.connection.MqConnection;
import com.tsa.mqmonitor.health.HealthEvaluator;
import com.tsa.mqmonitor.mq.MqGatewayFactory;
import jakarta.annotation.PreDestroy;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.TaskScheduler;
import org.springframework.stereotype.Component;

/**
 * Owns every sampler. A connection is sampled only while a browser watches it or while it has
 * Prometheus Export on; an idle deployment makes no MQ calls. State transitions happen under one
 * lock; MQ calls and gateway closes happen outside it.
 */
@Component
public class SamplerRegistry {
  private static final Logger log = LoggerFactory.getLogger(SamplerRegistry.class);

  private final ConnectionService connections;
  private final MqGatewayFactory factory;
  private final HealthEvaluator evaluator;
  private final TaskScheduler scheduler;
  private final Clock clock;
  private final Duration grace;
  private final Duration listInterval;
  private final List<SampleListener> listeners = new CopyOnWriteArrayList<>();
  private final Map<Long, Entry> entries = new ConcurrentHashMap<>();
  private final Set<String> listWatchers = new HashSet<>();
  private final Object lock = new Object();
  private ScheduledFuture<?> listTask;

  private static final class Entry {
    final Set<String> watchers = new HashSet<>();
    MqConnection connection;
    volatile Sampler sampler;
    ScheduledFuture<?> periodic;
    ScheduledFuture<?> pendingStop;

    Entry(MqConnection connection, Sampler sampler) {
      this.connection = connection;
      this.sampler = sampler;
    }
  }

  public SamplerRegistry(
      ConnectionService connections,
      MqGatewayFactory factory,
      HealthEvaluator evaluator,
      @Qualifier("samplerScheduler") TaskScheduler scheduler,
      Clock clock,
      MqMonitorProperties props) {
    this.connections = connections;
    this.factory = factory;
    this.evaluator = evaluator;
    this.scheduler = scheduler;
    this.clock = clock;
    this.grace = Duration.ofSeconds(props.sampling().watcherGraceSeconds());
    this.listInterval = Duration.ofSeconds(Math.max(1, props.sampling().listIntervalSeconds()));
  }

  public void addListener(SampleListener l) {
    listeners.add(l);
  }

  public Optional<EvaluatedSample> latest(long connectionId) {
    Entry e = entries.get(connectionId);
    return e == null ? Optional.empty() : e.sampler.latest();
  }

  public String watch(long connectionId) {
    String watcherId = UUID.randomUUID().toString();
    synchronized (lock) {
      Entry e = entries.get(connectionId);
      if (e == null) {
        MqConnection c =
            connections
                .findById(connectionId)
                .orElseThrow(
                    () -> new IllegalArgumentException("Unknown connection " + connectionId));
        e = newEntry(c);
      }
      e.watchers.add(watcherId);
      cancelPendingStop(e);
      start(e);
    }
    return watcherId;
  }

  public void unwatch(long connectionId, String watcherId) {
    synchronized (lock) {
      Entry e = entries.get(connectionId);
      if (e != null && e.watchers.remove(watcherId)) {
        scheduleStopIfIdle(connectionId, e);
      }
    }
  }

  public void watchList(String watcherId) {
    synchronized (lock) {
      listWatchers.add(watcherId);
      if (listTask == null) {
        listTask = scheduler.scheduleWithFixedDelay(safely(this::sampleList), listInterval);
      }
    }
  }

  public void unwatchList(String watcherId) {
    List<Sampler> toClose;
    synchronized (lock) {
      if (!listWatchers.remove(watcherId) || !listWatchers.isEmpty() || listTask == null) {
        return;
      }
      listTask.cancel(false);
      listTask = null;
      toClose = entries.values().stream().filter(e -> e.periodic == null).map(e -> e.sampler).toList();
    }
    toClose.forEach(Sampler::close);
  }

  public void connectionChanged(long id) {
    Optional<MqConnection> fresh = connections.findById(id);
    if (fresh.isEmpty()) {
      connectionDeleted(id);
      return;
    }
    List<Sampler> toClose = new ArrayList<>();
    synchronized (lock) {
      apply(fresh.get(), true, toClose);
    }
    toClose.forEach(Sampler::close);
  }

  public void connectionDeleted(long id) {
    Sampler toClose;
    synchronized (lock) {
      Entry e = entries.remove(id);
      if (e == null) {
        return;
      }
      toClose = stop(e);
    }
    toClose.close();
  }

  @EventListener(ApplicationReadyEvent.class)
  public void refreshPrometheusExports() {
    List<MqConnection> exports = connections.findPrometheusExports();
    Set<Long> exportIds = exports.stream().map(MqConnection::id).collect(Collectors.toSet());
    List<Long> stale =
        entries.entrySet().stream()
            .filter(x -> x.getValue().connection.prometheusExport())
            .map(Map.Entry::getKey)
            .filter(id -> !exportIds.contains(id))
            .toList();
    List<MqConnection> noLongerExporting = new ArrayList<>();
    for (Long id : stale) {
      connections.findById(id).ifPresentOrElse(noLongerExporting::add, () -> connectionDeleted(id));
    }
    List<Sampler> toClose = new ArrayList<>();
    synchronized (lock) {
      for (MqConnection c : exports) {
        apply(c, false, toClose);
      }
      for (MqConnection c : noLongerExporting) {
        apply(c, false, toClose);
      }
      entries.forEach(
          (id, e) -> {
            if (!exportIds.contains(id) && e.periodic != null && e.watchers.isEmpty()) {
              toClose.add(stop(e));
            }
          });
    }
    toClose.forEach(Sampler::close);
  }

  @PreDestroy
  public void shutdown() {
    List<Sampler> toClose = new ArrayList<>();
    synchronized (lock) {
      if (listTask != null) {
        listTask.cancel(false);
        listTask = null;
      }
      listWatchers.clear();
      entries.values().forEach(e -> toClose.add(stop(e)));
      entries.clear();
    }
    toClose.forEach(Sampler::close);
  }

  /** Brings an entry in line with a fresh connection row. Caller holds the lock. */
  private void apply(MqConnection fresh, boolean forceRebuild, List<Sampler> toClose) {
    Entry e = entries.get(fresh.id());
    if (e != null && (forceRebuild || !e.connection.equals(fresh))) {
      toClose.add(e.sampler);
      boolean wasRunning = e.periodic != null;
      if (wasRunning) {
        e.periodic.cancel(false);
        e.periodic = null;
      }
      e.connection = fresh;
      e.sampler = newSampler(fresh);
      if (wasRunning) {
        start(e);
      }
    }
    if (fresh.prometheusExport()) {
      if (e == null) {
        e = newEntry(fresh);
      }
      cancelPendingStop(e);
      start(e);
    } else if (e != null) {
      scheduleStopIfIdle(fresh.id(), e);
    }
  }

  private void sampleList() {
    List<MqConnection> all = connections.findAll();
    List<Entry> toRun = new ArrayList<>();
    synchronized (lock) {
      if (listTask == null) {
        return;
      }
      for (MqConnection c : all) {
        try {
          Entry e = entries.get(c.id());
          if (e == null) {
            e = newEntry(c);
          }
          if (e.periodic == null) {
            toRun.add(e);
          }
        } catch (RuntimeException ex) {
          log.warn("Cannot prepare list sample for connection {}: {}", c.id(), ex.toString());
        }
      }
    }
    for (Entry e : toRun) {
      Sampler s = e.sampler;
      s.sampleQueueManagerOnly();
      boolean idle;
      synchronized (lock) {
        idle = listTask == null && e.periodic == null;
      }
      if (idle) {
        s.close();
      }
    }
  }

  private Entry newEntry(MqConnection c) {
    Entry e = new Entry(c, newSampler(c));
    entries.put(c.id(), e);
    return e;
  }

  private Sampler newSampler(MqConnection c) {
    return new Sampler(
        c,
        connections.targetFor(c),
        connections.thresholdsFor(c),
        factory,
        evaluator,
        clock,
        listeners);
  }

  private void start(Entry e) {
    if (e.periodic == null) {
      Sampler s = e.sampler;
      Duration interval = Duration.ofSeconds(Math.max(1, e.connection.sampleIntervalSeconds()));
      e.periodic = scheduler.scheduleWithFixedDelay(safely(s::sampleOnce), interval);
    }
  }

  private void scheduleStopIfIdle(long id, Entry e) {
    if (e.watchers.isEmpty()
        && !e.connection.prometheusExport()
        && e.periodic != null
        && e.pendingStop == null) {
      e.pendingStop = scheduler.schedule(safely(() -> stopIfIdle(id, e)), clock.instant().plus(grace));
    }
  }

  private void stopIfIdle(long id, Entry e) {
    Sampler toClose = null;
    synchronized (lock) {
      if (entries.get(id) != e) {
        return;
      }
      e.pendingStop = null;
      if (e.watchers.isEmpty() && !e.connection.prometheusExport()) {
        toClose = stop(e);
      }
    }
    if (toClose != null) {
      toClose.close();
    }
  }

  /** Cancels the entry's tasks and returns its sampler for the caller to close. */
  private Sampler stop(Entry e) {
    if (e.periodic != null) {
      e.periodic.cancel(false);
      e.periodic = null;
    }
    cancelPendingStop(e);
    return e.sampler;
  }

  private void cancelPendingStop(Entry e) {
    if (e.pendingStop != null) {
      e.pendingStop.cancel(false);
      e.pendingStop = null;
    }
  }

  private static Runnable safely(Runnable task) {
    return () -> {
      try {
        task.run();
      } catch (RuntimeException ex) {
        log.warn("Sampling task failed: {}", ex.toString());
        log.debug("Sampling task failure detail", ex);
      }
    };
  }
}
