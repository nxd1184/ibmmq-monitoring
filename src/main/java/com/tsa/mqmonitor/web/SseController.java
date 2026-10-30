package com.tsa.mqmonitor.web;

import com.tsa.mqmonitor.connection.ConnectionService;
import com.tsa.mqmonitor.connection.MqConnection;
import com.tsa.mqmonitor.sampling.EvaluatedSample;
import com.tsa.mqmonitor.sampling.SampleListener;
import com.tsa.mqmonitor.sampling.SamplerRegistry;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
 * the {@link SamplerRegistry}; each way a stream can end (completion, timeout, error, failed send)
 * runs one idempotent close that releases that watch exactly once, so a closed tab never keeps a
 * sampler running.
 */
@RestController
public class SseController implements SampleListener {
  private static final Logger log = LoggerFactory.getLogger(SseController.class);

  private final SamplerRegistry registry;
  private final ConnectionService connections;
  private final SampleFragmentRenderer renderer;
  private final Map<Long, Set<Subscription>> byConnection = new ConcurrentHashMap<>();
  private final Set<Subscription> listSubscriptions = ConcurrentHashMap.newKeySet();

  /** One open stream. {@code connectionId} is null for a list-page stream. */
  private static final class Subscription {
    final SseEmitter emitter = new SseEmitter(0L);
    final Long connectionId;
    final String watcherId;
    final boolean showSystem;
    final AtomicBoolean closed = new AtomicBoolean();

    Subscription(Long connectionId, String watcherId, boolean showSystem) {
      this.connectionId = connectionId;
      this.watcherId = watcherId;
      this.showSystem = showSystem;
    }
  }

  public SseController(
      SamplerRegistry registry, ConnectionService connections, SampleFragmentRenderer renderer) {
    this.registry = registry;
    this.connections = connections;
    this.renderer = renderer;
  }

  @PostConstruct
  void subscribe() {
    registry.addListener(this);
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
    var sub = new Subscription(id, watcherId, showSystem);
    byConnection.computeIfAbsent(id, k -> ConcurrentHashMap.newKeySet()).add(sub);
    register(sub);
    try {
      registry.latest(id).ifPresent(s -> sendAll(sub, renderAll(s, showSystem)));
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
    var sub = new Subscription(null, watcherId, false);
    listSubscriptions.add(sub);
    register(sub);
    try {
      for (MqConnection c : connections.findAll()) {
        String row = renderer.renderRow(c, registry.latest(c.id()).orElse(null));
        if (!send(sub, rowEvent(c.id()), row)) {
          break;
        }
      }
    } catch (RuntimeException ex) {
      close(sub); // the emitter is never returned, so no callback would release the watch
      throw ex;
    }
    return sub.emitter;
  }

  @Override
  public void onSample(EvaluatedSample s) {
    long id = s.sample().connectionId();
    Set<Subscription> subs = byConnection.get(id);
    if (subs != null && !subs.isEmpty()) {
      Map<Boolean, Map<String, String>> rendered = new ConcurrentHashMap<>();
      for (Subscription sub : subs) {
        sendAll(sub, rendered.computeIfAbsent(sub.showSystem, flag -> renderAll(s, flag)));
      }
    }
    if (!listSubscriptions.isEmpty()) {
      connections
          .findById(id)
          .ifPresent(
              c -> {
                String row = renderer.renderRow(c, s);
                for (Subscription sub : listSubscriptions) {
                  send(sub, rowEvent(id), row);
                }
              });
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
    if (sub.connectionId == null) {
      listSubscriptions.remove(sub);
      registry.unwatchList(sub.watcherId);
    } else {
      byConnection.computeIfPresent(
          sub.connectionId,
          (k, set) -> {
            set.remove(sub);
            return set.isEmpty() ? null : set;
          });
      registry.unwatch(sub.connectionId, sub.watcherId);
    }
  }

  private Map<String, String> renderAll(EvaluatedSample s, boolean showSystem) {
    Map<String, String> events = new LinkedHashMap<>();
    for (String name : new String[] {"qmgr", "queues", "channels"}) {
      events.put(name, renderer.render(name, s, showSystem));
    }
    return events;
  }

  private void sendAll(Subscription sub, Map<String, String> events) {
    for (Map.Entry<String, String> e : events.entrySet()) {
      if (!send(sub, e.getKey(), e.getValue())) {
        return;
      }
    }
  }

  /** Sends one event; a failed send ends the stream. Returns false when the stream is closed. */
  private boolean send(Subscription sub, String event, String html) {
    if (sub.closed.get()) {
      return false;
    }
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
