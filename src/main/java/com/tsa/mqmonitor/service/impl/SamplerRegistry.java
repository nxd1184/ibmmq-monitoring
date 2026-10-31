package com.tsa.mqmonitor.service.impl;

import com.tsa.mqmonitor.config.MqMonitorProperties;
import com.tsa.mqmonitor.exception.UnknownConnectionException;
import com.tsa.mqmonitor.model.EvaluatedSample;
import com.tsa.mqmonitor.model.MqConnection;
import com.tsa.mqmonitor.service.ConnectionLifecycleListener;
import com.tsa.mqmonitor.service.MqGatewayFactory;
import com.tsa.mqmonitor.service.SampleListener;
import jakarta.annotation.PreDestroy;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
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
 * Prometheus Export on; an idle deployment makes no MQ calls.
 *
 * <p>State transitions happen under one lock; MQ calls and gateway closes happen outside it. A
 * sampler the registry stops is suspended under the lock (so a late run cannot reopen MQ) and its
 * gateway released afterwards; a sampler the registry discards is retired and never touches MQ
 * again. List-only samplers live in their own map and never create registry entries.
 */
@Component
public class SamplerRegistry {
  private static final Logger log = LoggerFactory.getLogger(SamplerRegistry.class);

  /** A sample older than this many sample intervals is stale. */
  public static final int STALE_AFTER_INTERVALS = 3;

  private final ConnectionService connectionService;
  private final MqGatewayFactory mqGatewayFactory;
  private final HealthEvaluator healthEvaluator;
  private final TaskScheduler taskScheduler;
  private final Clock clock;
  private final Duration grace;
  private final Duration listInterval;
  private final List<SampleListener> listeners = new CopyOnWriteArrayList<>();
  private final List<ConnectionLifecycleListener> lifecycleListeners = new CopyOnWriteArrayList<>();
  private final Map<Long, Entry> entries = new ConcurrentHashMap<>();
  private final Map<Long, Sampler> listSamplers = new ConcurrentHashMap<>();
  private final Set<Long> deletedSinceListRead = new HashSet<>();
  private final Set<String> listWatchers = new HashSet<>();
  private final Object lock = new Object();
  private ScheduledFuture<?> listTask;

  private static final class Entry {
    final Set<String> watchers = new HashSet<>();
    MqConnection connection;
    volatile Sampler sampler;
    ScheduledFuture<?> periodic;
    ScheduledFuture<?> pendingStop;
    /** One-shot, re-armed on every tick while {@code periodic} runs. */
    ScheduledFuture<?> watchdog;

    Entry(MqConnection connection, Sampler sampler) {
      this.connection = connection;
      this.sampler = sampler;
    }
  }

  public SamplerRegistry(
      ConnectionService connectionService,
      MqGatewayFactory mqGatewayFactory,
      HealthEvaluator healthEvaluator,
      @Qualifier("samplerScheduler") TaskScheduler taskScheduler,
      Clock clock,
      MqMonitorProperties mqMonitorProperties) {
    this.connectionService = connectionService;
    this.mqGatewayFactory = mqGatewayFactory;
    this.healthEvaluator = healthEvaluator;
    this.taskScheduler = taskScheduler;
    this.clock = clock;
    this.grace = Duration.ofSeconds(mqMonitorProperties.sampling().watcherGraceSeconds());
    this.listInterval = Duration.ofSeconds(Math.max(1, mqMonitorProperties.sampling().listIntervalSeconds()));
  }

  public void addListener(SampleListener l) {
    listeners.add(l);
  }

  public void addLifecycleListener(ConnectionLifecycleListener l) {
    lifecycleListeners.add(l);
  }

  public Optional<EvaluatedSample> latest(long connectionId) {
    Entry e = entries.get(connectionId);
    Optional<EvaluatedSample> full = e == null ? Optional.empty() : e.sampler.latest();
    if (full.isPresent()) {
      return full;
    }
    Sampler listOnly = listSamplers.get(connectionId);
    return listOnly == null ? Optional.empty() : listOnly.latest();
  }

  /**
   * True when {@code s} is older than {@link #STALE_AFTER_INTERVALS} sample intervals of its
   * connection. Used on first paint and stream open; running samplers are also checked by the
   * watchdog, which pushes {@link SampleListener#onStale}.
   */
  public boolean isStale(long connectionId, EvaluatedSample s) {
    if (s == null) {
      return false;
    }
    Entry e = entries.get(connectionId);
    MqConnection c = e != null ? e.connection : connectionService.findById(connectionId).orElse(null);
    return c != null && isStale(s, interval(c), clock.instant());
  }

  static boolean isStale(EvaluatedSample s, Duration interval, Instant now) {
    return Duration.between(s.sample().takenAt(), now)
            .compareTo(interval.multipliedBy(STALE_AFTER_INTERVALS))
        > 0;
  }

  private static Duration interval(MqConnection c) {
    return Duration.ofSeconds(Math.max(1, c.sampleIntervalSeconds()));
  }

  public String watch(long connectionId) {
    String watcherId = UUID.randomUUID().toString();
    Sampler toRetire;
    synchronized (lock) {
      Entry e = entries.get(connectionId);
      if (e == null) {
        MqConnection c =
            connectionService
                .findById(connectionId)
                .orElseThrow(() -> new UnknownConnectionException(connectionId));
        e = newEntry(c);
      }
      e.watchers.add(watcherId);
      cancelPendingStop(e);
      toRetire = start(connectionId, e);
    }
    retire(toRetire);
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
        listTask = taskScheduler.scheduleWithFixedDelay(safely(this::sampleList), listInterval);
      }
    }
  }

  public void unwatchList(String watcherId) {
    List<Sampler> toRetire;
    synchronized (lock) {
      if (!listWatchers.remove(watcherId) || !listWatchers.isEmpty() || listTask == null) {
        return;
      }
      listTask.cancel(false);
      listTask = null;
      toRetire = new ArrayList<>(listSamplers.values());
      listSamplers.clear();
      deletedSinceListRead.clear();
    }
    toRetire.forEach(Sampler::retire);
  }

  public void connectionChanged(long id) {
    Optional<MqConnection> fresh = connectionService.findById(id);
    if (fresh.isEmpty()) {
      connectionDeleted(id);
      return;
    }
    List<Sampler> toRetire = new ArrayList<>();
    List<Sampler> toRelease = new ArrayList<>();
    synchronized (lock) {
      apply(fresh.get(), true, toRetire, toRelease);
      Sampler listOnly = listSamplers.remove(id);
      if (listOnly != null) {
        toRetire.add(listOnly);
      }
    }
    toRetire.forEach(Sampler::retire);
    toRelease.forEach(Sampler::releaseGateway);
    if (!fresh.get().prometheusExport()) {
      notifyRemoved(id);
    }
  }

  public void connectionDeleted(long id) {
    List<Sampler> toRetire = new ArrayList<>();
    synchronized (lock) {
      if (listTask != null) {
        deletedSinceListRead.add(id);
      }
      Entry e = entries.remove(id);
      if (e != null) {
        cancelTasks(e);
        toRetire.add(e.sampler);
      }
      Sampler listOnly = listSamplers.remove(id);
      if (listOnly != null) {
        toRetire.add(listOnly);
      }
    }
    toRetire.forEach(Sampler::retire);
    notifyRemoved(id);
  }

  private void notifyRemoved(long id) {
    for (ConnectionLifecycleListener l : lifecycleListeners) {
      try {
        l.connectionRemoved(id);
      } catch (RuntimeException ex) {
        log.warn("Lifecycle listener failed for connection {}: {}", id, ex.toString());
      }
    }
  }

  @EventListener(ApplicationReadyEvent.class)
  public void refreshPrometheusExports() {
    List<MqConnection> exports = connectionService.findPrometheusExports();
    Set<Long> exportIds = exports.stream().map(MqConnection::id).collect(Collectors.toSet());
    List<Long> stale =
        entries.entrySet().stream()
            .filter(x -> x.getValue().connection.prometheusExport())
            .map(Map.Entry::getKey)
            .filter(id -> !exportIds.contains(id))
            .toList();
    List<MqConnection> noLongerExporting = new ArrayList<>();
    for (Long id : stale) {
      connectionService.findById(id).ifPresentOrElse(noLongerExporting::add, () -> connectionDeleted(id));
    }
    List<Sampler> toRetire = new ArrayList<>();
    List<Sampler> toRelease = new ArrayList<>();
    synchronized (lock) {
      for (MqConnection c : exports) {
        apply(c, false, toRetire, toRelease);
      }
      for (MqConnection c : noLongerExporting) {
        apply(c, false, toRetire, toRelease);
      }
      entries.forEach(
          (id, e) -> {
            if (!exportIds.contains(id) && e.periodic != null && e.watchers.isEmpty()) {
              toRelease.add(stop(e));
            }
          });
    }
    toRetire.forEach(Sampler::retire);
    toRelease.forEach(Sampler::releaseGateway);
  }

  @PreDestroy
  public void shutdown() {
    List<Sampler> toRetire = new ArrayList<>();
    synchronized (lock) {
      if (listTask != null) {
        listTask.cancel(false);
        listTask = null;
      }
      listWatchers.clear();
      entries.values().forEach(e -> {
        cancelTasks(e);
        toRetire.add(e.sampler);
      });
      entries.clear();
      toRetire.addAll(listSamplers.values());
      listSamplers.clear();
    }
    toRetire.forEach(Sampler::retire);
  }

  /** Brings an entry in line with a fresh connection row. Caller holds the lock. */
  private void apply(
      MqConnection fresh, boolean forceRebuild, List<Sampler> toRetire, List<Sampler> toRelease) {
    Entry e = entries.get(fresh.id());
    if (e != null && (forceRebuild || !e.connection.equals(fresh))) {
      toRetire.add(e.sampler);
      boolean wasRunning = e.periodic != null;
      if (wasRunning) {
        e.periodic.cancel(false);
        e.periodic = null;
      }
      cancelWatchdog(e);
      e.connection = fresh;
      e.sampler = newSampler(fresh);
      if (wasRunning) {
        addIfPresent(toRetire, start(fresh.id(), e));
      }
    }
    if (fresh.prometheusExport()) {
      if (e == null) {
        e = newEntry(fresh);
      }
      cancelPendingStop(e);
      addIfPresent(toRetire, start(fresh.id(), e));
    } else if (e != null) {
      scheduleStopIfIdle(fresh.id(), e);
    }
  }

  private void sampleList() {
    synchronized (lock) {
      if (listTask == null) {
        return;
      }
      deletedSinceListRead.clear();
    }
    List<MqConnection> all = connectionService.findAll();
    Map<Long, Sampler> toRun = new LinkedHashMap<>();
    List<Sampler> toRetire = new ArrayList<>();
    synchronized (lock) {
      if (listTask == null) {
        return;
      }
      Set<Long> ids = all.stream().map(MqConnection::id).collect(Collectors.toSet());
      listSamplers.keySet().stream()
          .filter(id -> !ids.contains(id))
          .toList()
          .forEach(id -> toRetire.add(listSamplers.remove(id)));
      for (MqConnection c : all) {
        long id = c.id();
        if (deletedSinceListRead.contains(id)) {
          continue;
        }
        Entry e = entries.get(id);
        if (e != null && e.periodic != null) {
          addIfPresent(toRetire, listSamplers.remove(id));
          continue;
        }
        try {
          toRun.put(id, listSamplers.computeIfAbsent(id, x -> newSampler(c)));
        } catch (RuntimeException ex) {
          log.warn("Cannot prepare list sample for connection {}: {}", id, ex.toString());
        }
      }
    }
    toRetire.forEach(Sampler::retire);
    toRun.forEach(
        (id, s) -> {
          s.sampleQueueManagerOnly();
          boolean current;
          synchronized (lock) {
            current = listTask != null && listSamplers.get(id) == s;
          }
          if (!current) {
            s.retire();
          }
        });
  }

  private Entry newEntry(MqConnection c) {
    Entry e = new Entry(c, newSampler(c));
    entries.put(c.id(), e);
    return e;
  }

  private Sampler newSampler(MqConnection c) {
    return new Sampler(
        c,
        connectionService.targetFor(c),
        connectionService.thresholdsFor(c),
        mqGatewayFactory,
        healthEvaluator,
        clock,
        listeners);
  }

  /**
   * Starts the entry's periodic task if it is not running. A full sampler replaces list-only
   * sampling for the connection, so the list-only sampler (if any) is returned to be retired.
   */
  private Sampler start(long id, Entry e) {
    if (e.periodic == null) {
      Sampler s = e.sampler;
      s.resume();
      Duration interval = interval(e.connection);
      e.periodic = taskScheduler.scheduleWithFixedDelay(safely(s::sampleOnce), interval);
      armWatchdog(id, e, interval);
    }
    return listSamplers.remove(id);
  }

  /** Caller holds the lock. */
  private void armWatchdog(long id, Entry e, Duration interval) {
    cancelWatchdog(e);
    e.watchdog =
        taskScheduler.schedule(safely(() -> watchdogTick(id, e)), clock.instant().plus(interval));
  }

  /**
   * Runs once per sample interval while the entry samples. A sample that hangs or a starved
   * scheduler leaves {@code latest} behind; listeners then get a stale push every tick, which
   * also serves as the SSE heartbeat for an idle stream.
   */
  private void watchdogTick(long id, Entry e) {
    EvaluatedSample latest;
    Duration interval;
    synchronized (lock) {
      if (entries.get(id) != e || e.periodic == null) {
        return;
      }
      interval = interval(e.connection);
      armWatchdog(id, e, interval);
      latest = e.sampler.latest().orElse(null);
    }
    if (latest != null && isStale(latest, interval, clock.instant())) {
      for (SampleListener l : listeners) {
        try {
          l.onStale(latest);
        } catch (RuntimeException ex) {
          log.warn("Stale listener failed for connection {}: {}", id, ex.toString());
        }
      }
    }
  }

  private void cancelWatchdog(Entry e) {
    if (e.watchdog != null) {
      e.watchdog.cancel(false);
      e.watchdog = null;
    }
  }

  private void scheduleStopIfIdle(long id, Entry e) {
    if (e.watchers.isEmpty()
        && !e.connection.prometheusExport()
        && e.periodic != null
        && e.pendingStop == null) {
      e.pendingStop =
          taskScheduler.schedule(safely(() -> stopIfIdle(id, e)), clock.instant().plus(grace));
    }
  }

  private void stopIfIdle(long id, Entry e) {
    Sampler toRelease = null;
    synchronized (lock) {
      if (entries.get(id) != e) {
        return;
      }
      e.pendingStop = null;
      if (e.watchers.isEmpty() && !e.connection.prometheusExport()) {
        toRelease = stop(e);
      }
    }
    if (toRelease != null) {
      toRelease.releaseGateway();
    }
  }

  /**
   * Cancels the entry's tasks and suspends its sampler so no run can reopen MQ; the caller
   * releases the returned sampler's gateway outside the lock. The sampler is reused on restart.
   */
  private Sampler stop(Entry e) {
    cancelTasks(e);
    e.sampler.suspend();
    return e.sampler;
  }

  private void cancelTasks(Entry e) {
    if (e.periodic != null) {
      e.periodic.cancel(false);
      e.periodic = null;
    }
    cancelWatchdog(e);
    cancelPendingStop(e);
  }

  private void cancelPendingStop(Entry e) {
    if (e.pendingStop != null) {
      e.pendingStop.cancel(false);
      e.pendingStop = null;
    }
  }

  private static void retire(Sampler s) {
    if (s != null) {
      s.retire();
    }
  }

  private static void addIfPresent(List<Sampler> list, Sampler s) {
    if (s != null) {
      list.add(s);
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
