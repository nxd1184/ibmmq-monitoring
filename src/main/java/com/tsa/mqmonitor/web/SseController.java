package com.tsa.mqmonitor.web;

import com.tsa.mqmonitor.connection.ConnectionService;
import com.tsa.mqmonitor.connection.MqConnection;
import com.tsa.mqmonitor.sampling.EvaluatedSample;
import com.tsa.mqmonitor.sampling.SampleListener;
import com.tsa.mqmonitor.sampling.SamplerRegistry;
import jakarta.annotation.PostConstruct;
import jakarta.annotation.PreDestroy;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

/**
 * Pushes rendered fragments to browsers over Server-Sent Events. Every stream holds a watch on
 * the {@link SamplerRegistry}; each way a stream can end (completion, timeout, error, failed send,
 * shutdown) runs one idempotent close that releases that watch exactly once, so a closed tab
 * never keeps a sampler running.
 *
 * <p>Sampler threads only render and enqueue. Each subscription has a coalescing outbox (latest
 * HTML per event name) drained by at most one task at a time on a small send pool, so a stalled
 * browser never blocks sampling or other subscribers, and a busy subscription skips stale
 * fragments in favour of the newest.
 */
@RestController
public class SseController implements SampleListener {
  private static final Logger log = LoggerFactory.getLogger(SseController.class);
  private static final int SEND_THREADS = 4;

  private final SamplerRegistry registry;
  private final ConnectionService connections;
  private final SampleFragmentRenderer renderer;
  private final Supplier<SseEmitter> emitterFactory;
  private final ExecutorService sendPool = Executors.newFixedThreadPool(SEND_THREADS, daemonThreads());
  private final Map<Long, Set<Subscription>> byConnection = new ConcurrentHashMap<>();
  private final Set<Subscription> listSubscriptions = ConcurrentHashMap.newKeySet();

  /** One open stream. {@code connectionId} is null for a list-page stream. */
  private static final class Subscription {
    final SseEmitter emitter;
    final Long connectionId;
    final String watcherId;
    final boolean showSystem;
    final AtomicBoolean closed = new AtomicBoolean();
    final AtomicBoolean draining = new AtomicBoolean();
    /** Newest unsent HTML per event name; a newer sample overwrites an unsent older one. */
    final Map<String, String> outbox = new ConcurrentHashMap<>();

    Subscription(SseEmitter emitter, Long connectionId, String watcherId, boolean showSystem) {
      this.emitter = emitter;
      this.connectionId = connectionId;
      this.watcherId = watcherId;
      this.showSystem = showSystem;
    }
  }

  @Autowired
  public SseController(
      SamplerRegistry registry, ConnectionService connections, SampleFragmentRenderer renderer) {
    this(registry, connections, renderer, () -> new SseEmitter(0L));
  }

  SseController(
      SamplerRegistry registry,
      ConnectionService connections,
      SampleFragmentRenderer renderer,
      Supplier<SseEmitter> emitterFactory) {
    this.registry = registry;
    this.connections = connections;
    this.renderer = renderer;
    this.emitterFactory = emitterFactory;
  }

  @PostConstruct
  void subscribe() {
    registry.addListener(this);
  }

  /** Completes every open stream so shutdown is not held by never-ending requests. */
  @PreDestroy
  void shutdown() {
    List<Subscription> all = new ArrayList<>(listSubscriptions);
    byConnection.values().forEach(all::addAll);
    for (Subscription sub : all) {
      try {
        sub.emitter.complete();
      } catch (RuntimeException ignored) {
        // already completed
      }
      close(sub);
    }
    sendPool.shutdownNow();
  }

  @GetMapping(path = "/connections/{id}/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public SseEmitter stream(
      @PathVariable long id, @RequestParam(defaultValue = "false") boolean showSystem) {
    String watcherId;
    try {
      watcherId = registry.watch(id);
    } catch (IllegalArgumentException unknown) {
      throw new ResponseStatusException(HttpStatus.NOT_FOUND);
    }
    var sub = new Subscription(emitterFactory.get(), id, watcherId, showSystem);
    byConnection.compute(
        id,
        (k, set) -> {
          Set<Subscription> s = set == null ? ConcurrentHashMap.newKeySet() : set;
          s.add(sub);
          return s;
        });
    register(sub);
    try {
      registry
          .latest(id)
          .ifPresent(s -> enqueue(sub, renderAll(s, showSystem, registry.isStale(id, s))));
    } catch (RuntimeException ex) {
      close(sub); // the emitter is never returned, so no callback would release the watch
      throw ex;
    }
    return sub.emitter;
  }

  @GetMapping(path = "/connections/stream", produces = MediaType.TEXT_EVENT_STREAM_VALUE)
  public SseEmitter listStream() {
    String watcherId = UUID.randomUUID().toString();
    registry.watchList(watcherId);
    var sub = new Subscription(emitterFactory.get(), null, watcherId, false);
    listSubscriptions.add(sub);
    register(sub);
    try {
      Map<String, String> rows = new LinkedHashMap<>();
      for (MqConnection c : connections.findAll()) {
        rows.put(rowEvent(c.id()), renderer.renderRow(c, registry.latest(c.id()).orElse(null)));
      }
      enqueue(sub, rows);
    } catch (RuntimeException ex) {
      close(sub); // the emitter is never returned, so no callback would release the watch
      throw ex;
    }
    return sub.emitter;
  }

  /** Runs on a sampler thread: renders once per variant and enqueues; never writes to a socket. */
  @Override
  public void onSample(EvaluatedSample s) {
    long id = s.sample().connectionId();
    Set<Subscription> subs = byConnection.get(id);
    if (subs != null && !subs.isEmpty()) {
      Map<Boolean, Map<String, String>> rendered = new LinkedHashMap<>();
      for (Subscription sub : subs) {
        enqueue(sub, rendered.computeIfAbsent(sub.showSystem, flag -> renderAll(s, flag, false)));
      }
    }
    if (!listSubscriptions.isEmpty()) {
      connections
          .findById(id)
          .ifPresent(
              c -> {
                Map<String, String> row = Map.of(rowEvent(id), renderer.renderRow(c, s));
                for (Subscription sub : listSubscriptions) {
                  enqueue(sub, row);
                }
              });
    }
  }

  /**
   * Runs on a sampler thread each watchdog tick while the newest sample is too old: replaces the
   * dashboard header with "Stale · last sample <time>". The periodic push is also the heartbeat
   * of an otherwise idle stream.
   */
  @Override
  public void onStale(EvaluatedSample latest) {
    long id = latest.sample().connectionId();
    Set<Subscription> subs = byConnection.get(id);
    if (subs == null || subs.isEmpty()) {
      return;
    }
    try {
      Map<String, String> header = Map.of("qmgr", renderer.render("qmgr", latest, false, true));
      for (Subscription sub : subs) {
        enqueue(sub, header);
      }
    } catch (RuntimeException ex) {
      log.warn("Stale push failed for connection {}: {}", id, ex.toString());
    }
  }

  private void register(Subscription sub) {
    Runnable close = () -> close(sub);
    sub.emitter.onCompletion(close);
    sub.emitter.onTimeout(close);
    sub.emitter.onError(ex -> close(sub));
  }

  /** Idempotent: removes the subscription and releases its watch exactly once. */
  private void close(Subscription sub) {
    if (!sub.closed.compareAndSet(false, true)) {
      return;
    }
    sub.outbox.clear();
    if (sub.connectionId == null) {
      listSubscriptions.remove(sub);
      registry.unwatchList(sub.watcherId);
    } else {
      byConnection.compute(
          sub.connectionId,
          (k, set) -> {
            if (set == null) {
              return null;
            }
            set.remove(sub);
            return set.isEmpty() ? null : set;
          });
      registry.unwatch(sub.connectionId, sub.watcherId);
    }
  }

  private Map<String, String> renderAll(EvaluatedSample s, boolean showSystem, boolean stale) {
    Map<String, String> events = new LinkedHashMap<>();
    for (String name : new String[] {"qmgr", "queues", "channels"}) {
      events.put(name, renderer.render(name, s, showSystem, stale));
    }
    return events;
  }

  private void enqueue(Subscription sub, Map<String, String> events) {
    if (sub.closed.get()) {
      return;
    }
    sub.outbox.putAll(events);
    scheduleDrain(sub);
  }

  private void scheduleDrain(Subscription sub) {
    if (!sub.draining.compareAndSet(false, true)) {
      return; // the running drain picks up what was just enqueued
    }
    try {
      sendPool.execute(() -> drain(sub));
    } catch (RejectedExecutionException shuttingDown) {
      sub.draining.set(false);
      close(sub);
    }
  }

  /** At most one drain per subscription runs at a time, which keeps its sends ordered. */
  private void drain(Subscription sub) {
    try {
      while (!sub.closed.get()) {
        Iterator<String> names = sub.outbox.keySet().iterator();
        if (!names.hasNext()) {
          break;
        }
        String event = names.next();
        String html = sub.outbox.remove(event);
        if (html != null && !send(sub, event, html)) {
          break;
        }
      }
    } finally {
      sub.draining.set(false);
      if (!sub.closed.get() && !sub.outbox.isEmpty()) {
        scheduleDrain(sub); // something arrived after the loop saw an empty outbox
      }
    }
  }

  /** Sends one event; a failed send ends the stream. Returns false when the stream is closed. */
  private boolean send(Subscription sub, String event, String html) {
    SseEmitter.SseEventBuilder builder = SseEmitter.event().name(event);
    // One data line per HTML line: a raw newline inside a data field would break SSE framing.
    html.lines().forEach(builder::data);
    try {
      sub.emitter.send(builder);
      return true;
    } catch (IOException | IllegalStateException ex) {
      log.debug("SSE send failed, closing stream: {}", ex.toString());
      try {
        sub.emitter.completeWithError(ex);
      } catch (RuntimeException ignored) {
        // already completed
      }
      close(sub);
      return false;
    }
  }

  private static String rowEvent(long id) {
    return "conn-" + id;
  }

  private static ThreadFactory daemonThreads() {
    AtomicInteger n = new AtomicInteger();
    return r -> {
      Thread t = new Thread(r, "sse-send-" + n.incrementAndGet());
      t.setDaemon(true);
      return t;
    };
  }

  // Test hooks: package-visible views of the open streams.

  Set<SseEmitter> emittersFor(long id) {
    return byConnection.getOrDefault(id, Set.of()).stream()
        .map(s -> s.emitter)
        .collect(Collectors.toSet());
  }

  Set<SseEmitter> listEmitters() {
    return listSubscriptions.stream().map(s -> s.emitter).collect(Collectors.toSet());
  }

  Set<String> listWatcherIds() {
    return listSubscriptions.stream().map(s -> s.watcherId).collect(Collectors.toSet());
  }
}
