package com.tsa.mqmonitor.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.asyncDispatch;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.request;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.tsa.mqmonitor.health.HealthVerdict;
import com.tsa.mqmonitor.mq.QueueInfo;
import com.tsa.mqmonitor.mq.QueueManagerInfo;
import com.tsa.mqmonitor.sampling.EvaluatedSample;
import com.tsa.mqmonitor.sampling.Sample;
import com.tsa.mqmonitor.sampling.SamplerRegistry;
import com.tsa.mqmonitor.sampling.UnknownConnectionException;
import com.tsa.mqmonitor.support.PostgresTestBase;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;
import com.tsa.mqmonitor.connection.ConnectionService;
import jakarta.servlet.AsyncEvent;
import java.io.IOException;
import org.springframework.mock.web.MockAsyncContext;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

class SseControllerTest extends PostgresTestBase {
  @Autowired MockMvc mvc;
  @Autowired SseController controller;
  @MockitoBean SamplerRegistry registry;

  static EvaluatedSample sample(long id) {
    var s =
        new Sample(
            id,
            Instant.parse("2026-10-30T10:15:30Z"),
            new QueueManagerInfo("QM1", "RUNNING", 940, "UNIX"),
            List.of(new QueueInfo("DEV.Q1", 12345, 5000, 1, 1, null, null, null)),
            List.of(),
            null);
    return new EvaluatedSample(
        s, HealthVerdict.healthy(), Map.of("DEV.Q1", HealthVerdict.healthy()), Map.of());
  }

  static void await(String what, BooleanSupplier condition) throws InterruptedException {
    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("Timed out waiting for " + what);
      }
      Thread.sleep(5);
    }
  }

  private static BooleanSupplier bodyContains(MvcResult result, String text) {
    return () -> {
      try {
        return result.getResponse().getContentAsString().contains(text);
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    };
  }

  /** Emitter that records sends (optionally blocking first) and exposes the completion hook. */
  static final class TestEmitter extends SseEmitter {
    final List<Runnable> completions = new CopyOnWriteArrayList<>();
    final List<String> sent = new CopyOnWriteArrayList<>();
    final CountDownLatch release;

    TestEmitter(CountDownLatch release) {
      super(0L);
      this.release = release;
    }

    @Override
    public synchronized void onCompletion(Runnable callback) {
      completions.add(callback);
      super.onCompletion(callback);
    }

    /** What the container does when the client goes away. */
    void fireCompletion() {
      completions.forEach(Runnable::run);
    }

    @Override
    public void send(SseEventBuilder builder) throws java.io.IOException {
      if (release != null) {
        try {
          release.await();
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new java.io.IOException(e);
        }
      }
      sent.add(builder.build().stream().map(d -> d.getData().toString()).collect(Collectors.joining()));
    }

    boolean received(String text) {
      return sent.stream().anyMatch(x -> x.contains(text));
    }
  }

  private static SseController standalone(SamplerRegistry reg, BlockingQueue<TestEmitter> emitters) {
    return new SseController(
        reg,
        mock(ConnectionService.class),
        new SampleFragmentRenderer(SampleFragmentRendererTest.engine()),
        () -> {
          TestEmitter next = emitters.poll();
          return next != null ? next : new TestEmitter(null);
        });
  }

  private SseEmitter onlyEmitter(long id) {
    var set = controller.emittersFor(id);
    assertThat(set).hasSize(1);
    return set.iterator().next();
  }

  /** Completes the emitter and lets MockMvc finish the async request, as a container would. */
  private void finish(MvcResult result, SseEmitter emitter) throws Exception {
    emitter.complete();
    mvc.perform(asyncDispatch(result));
  }

  @Test
  void streamWatchesThenUnwatchesOnCompletion() throws Exception {
    when(registry.watch(1L)).thenReturn("w1");
    MvcResult result =
        mvc.perform(get("/connections/1/stream").with(user("admin")))
            .andExpect(request().asyncStarted())
            .andReturn();
    verify(registry).watch(1L);
    verify(registry, never()).unwatch(1L, "w1");

    var emitter = onlyEmitter(1L);
    finish(result, emitter);
    emitter.complete();

    verify(registry, times(1)).unwatch(1L, "w1");
    assertThat(controller.emittersFor(1L)).isEmpty();
  }

  @Test
  void clientDisconnectUnwatchesOnce() throws Exception {
    when(registry.watch(4L)).thenReturn("w4");
    MvcResult result =
        mvc.perform(get("/connections/4/stream").with(user("admin")))
            .andExpect(request().asyncStarted())
            .andReturn();
    var async = (MockAsyncContext) result.getRequest().getAsyncContext();
    var event = new AsyncEvent(async, new IOException("Broken pipe"));
    for (var l : async.getListeners()) {
      l.onError(event);
    }
    for (var l : async.getListeners()) {
      l.onComplete(event);
    }

    verify(registry, times(1)).unwatch(4L, "w4");
    assertThat(controller.emittersFor(4L)).isEmpty();
  }

  @Test
  void failedSendClosesTheStream() throws Exception {
    when(registry.watch(5L)).thenReturn("w5");
    mvc.perform(get("/connections/5/stream").with(user("admin")))
        .andExpect(request().asyncStarted());
    onlyEmitter(5L).complete(); // a later send now throws IllegalStateException

    controller.onSample(sample(5L));
    verify(registry, timeout(2000)).unwatch(5L, "w5");
    controller.onSample(sample(5L));

    verify(registry, times(1)).unwatch(5L, "w5");
    assertThat(controller.emittersFor(5L)).isEmpty();
  }

  @Test
  void firstEventIsLatestSampleWhenPresent() throws Exception {
    when(registry.watch(1L)).thenReturn("w1");
    when(registry.latest(1L)).thenReturn(Optional.of(sample(1L)));
    MvcResult result =
        mvc.perform(get("/connections/1/stream").with(user("admin")))
            .andExpect(request().asyncStarted())
            .andReturn();
    await("initial events", bodyContains(result, "event:channels"));
    finish(result, onlyEmitter(1L));
    verify(registry).unwatch(1L, "w1");

    String body = result.getResponse().getContentAsString();
    assertThat(body).contains("event:qmgr").contains("event:queues").contains("event:channels");
    assertThat(body).contains("12345");
  }

  @Test
  void laterSamplesArePushedToTheConnectionsEmitters() throws Exception {
    when(registry.watch(2L)).thenReturn("w2");
    MvcResult result =
        mvc.perform(get("/connections/2/stream").with(user("admin")))
            .andExpect(request().asyncStarted())
            .andReturn();
    controller.onSample(sample(3L));
    controller.onSample(sample(2L));
    await("pushed sample", bodyContains(result, "12345"));
    finish(result, onlyEmitter(2L));

    String body = result.getResponse().getContentAsString();
    assertThat(body).contains("event:queues").contains("12345");
    verify(registry).unwatch(2L, "w2");
  }

  @Test
  void decryptFailureIsAServerErrorNot404() {
    when(registry.watch(7L)).thenThrow(new IllegalArgumentException("stored secret was tampered with"));
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> mvc.perform(get("/connections/7/stream").with(user("admin"))))
        .hasRootCauseInstanceOf(IllegalArgumentException.class);
    assertThat(controller.emittersFor(7L)).isEmpty();
  }

  @Test
  void stalePushReplacesTheHeader() throws Exception {
    when(registry.watch(8L)).thenReturn("w8");
    MvcResult result =
        mvc.perform(get("/connections/8/stream").with(user("admin")))
            .andExpect(request().asyncStarted())
            .andReturn();
    controller.onStale(sample(8L));
    await("stale header", bodyContains(result, "live-stale"));
    finish(result, onlyEmitter(8L));
    assertThat(result.getResponse().getContentAsString())
        .contains("event:qmgr")
        .contains("live-stale")
        .doesNotContain("event:queues");
  }

  @Test
  void streamOpenRendersStaleWhenRetainedSampleIsOld() throws Exception {
    when(registry.watch(9L)).thenReturn("w9");
    when(registry.latest(9L)).thenReturn(Optional.of(sample(9L)));
    when(registry.isStale(org.mockito.ArgumentMatchers.eq(9L), org.mockito.ArgumentMatchers.any()))
        .thenReturn(true);
    MvcResult result =
        mvc.perform(get("/connections/9/stream").with(user("admin")))
            .andExpect(request().asyncStarted())
            .andReturn();
    await("initial events", bodyContains(result, "event:channels"));
    finish(result, onlyEmitter(9L));
    assertThat(result.getResponse().getContentAsString())
        .contains("live-stale")
        .contains("last sample")
        .doesNotContain("live-word");
  }

  @Test
  void failingDashboardFanOutStillPushesListRow() throws Exception {
    var reg = mock(SamplerRegistry.class);
    var service = mock(ConnectionService.class);
    var renderer = mock(SampleFragmentRenderer.class);
    when(renderer.render(anyString(), org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.anyBoolean(),
            org.mockito.ArgumentMatchers.anyBoolean()))
        .thenThrow(new IllegalStateException("template broken"));
    when(renderer.renderRow(org.mockito.ArgumentMatchers.any(), org.mockito.ArgumentMatchers.any()))
        .thenReturn("<tr>row</tr>");
    when(reg.watch(10L)).thenReturn("w10");
    when(reg.latest(org.mockito.ArgumentMatchers.anyLong())).thenReturn(Optional.empty());
    var conn = SampleFragmentRendererTest.connection(10L);
    when(service.findById(10L)).thenReturn(Optional.of(conn));
    when(service.findAll()).thenReturn(List.of());
    var emitters = new ArrayBlockingQueue<TestEmitter>(2);
    var dash = new TestEmitter(null);
    var list = new TestEmitter(null);
    emitters.add(dash);
    emitters.add(list);
    var c = new SseController(reg, service, renderer, emitters::poll);
    c.stream(10L, false);
    c.listStream();
    c.onSample(sample(10L));
    await("list row", () -> list.received("row"));
    c.shutdown();
  }

  @Test
  void failureBeforeTheEmitterIsReturnedStillUnwatches() {
    when(registry.watch(6L)).thenReturn("w6");
    when(registry.latest(6L)).thenThrow(new IllegalStateException("boom"));
    org.assertj.core.api.Assertions.assertThatThrownBy(
            () -> mvc.perform(get("/connections/6/stream").with(user("admin"))))
        .hasRootCauseMessage("boom");
    verify(registry, times(1)).unwatch(6L, "w6");
    assertThat(controller.emittersFor(6L)).isEmpty();
  }

  @Test
  void unknownConnectionIs404() throws Exception {
    when(registry.watch(99L)).thenThrow(new UnknownConnectionException(99L));
    mvc.perform(get("/connections/99/stream").with(user("admin")))
        .andExpect(status().isNotFound());
    assertThat(controller.emittersFor(99L)).isEmpty();
  }

  @Test
  void streamRequiresLogin() throws Exception {
    mvc.perform(get("/connections/1/stream")).andExpect(status().isUnauthorized());
    verify(registry, never()).watch(1L);
  }

  @Test
  void listStreamWatchesAndUnwatchesList() throws Exception {
    MvcResult result =
        mvc.perform(get("/connections/stream").with(user("admin")))
            .andExpect(request().asyncStarted())
            .andReturn();
    var watchers = controller.listWatcherIds();
    assertThat(watchers).hasSize(1);
    String watcherId = watchers.iterator().next();
    verify(registry).watchList(watcherId);

    var emitter = controller.listEmitters().iterator().next();
    finish(result, emitter);
    emitter.complete();

    verify(registry, times(1)).unwatchList(watcherId);
    assertThat(controller.listEmitters()).isEmpty();
  }

  @Test
  void slowSubscriberDoesNotBlockSampler() throws Exception {
    var reg = mock(SamplerRegistry.class);
    var ids = new AtomicInteger();
    when(reg.watch(1L)).thenAnswer(inv -> "s" + ids.incrementAndGet());
    var latch = new CountDownLatch(1);
    var slow = new TestEmitter(latch);
    var fast = new TestEmitter(null);
    var c = standalone(reg, new ArrayBlockingQueue<>(2, false, List.of(slow, fast)));
    try {
      assertThat(c.stream(1L, false)).isSameAs(slow);
      assertThat(c.stream(1L, false)).isSameAs(fast);

      long start = System.nanoTime();
      c.onSample(sample(1L));
      c.onSample(sample(1L));
      assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)).isLessThan(1000);

      await("fast subscriber event", () -> fast.received("event:queues"));
      assertThat(slow.sent).isEmpty();
      latch.countDown();
      await("slow subscriber event", () -> slow.received("event:queues"));
      assertThat(fast.received("12345")).isTrue();
    } finally {
      latch.countDown();
      c.shutdown();
    }
  }

  @Test
  void preDestroyCompletesEmittersAndUnwatches() throws Exception {
    var reg = mock(SamplerRegistry.class);
    when(reg.watch(1L)).thenReturn("p1");
    var c = standalone(reg, new ArrayBlockingQueue<>(1));
    c.stream(1L, false);
    c.listStream();
    String listWatcher = c.listWatcherIds().iterator().next();

    c.shutdown();

    verify(reg, times(1)).unwatch(1L, "p1");
    verify(reg, times(1)).unwatchList(listWatcher);
    assertThat(c.emittersFor(1L)).isEmpty();
    assertThat(c.listEmitters()).isEmpty();
  }

  @Test
  void concurrentOpenAndCloseNeverOrphansSubscription() throws Exception {
    int n = 300;
    var reg = mock(SamplerRegistry.class);
    var ids = new AtomicInteger();
    when(reg.watch(1L)).thenAnswer(inv -> "c" + ids.incrementAndGet());
    var c = standalone(reg, new ArrayBlockingQueue<>(1));
    ExecutorService closer = Executors.newSingleThreadExecutor();
    try {
      var live = (TestEmitter) c.stream(1L, false);
      for (int i = 0; i < n; i++) {
        TestEmitter old = live;
        var go = new CountDownLatch(1);
        Future<?> closing =
            closer.submit(
                () -> {
                  go.await();
                  old.fireCompletion();
                  return null;
                });
        go.countDown();
        live = (TestEmitter) c.stream(1L, false);
        closing.get(5, TimeUnit.SECONDS);
        assertThat(c.emittersFor(1L)).containsExactly(live);
      }
      verify(reg, times(n + 1)).watch(1L);
      verify(reg, times(n)).unwatch(eq(1L), anyString());

      TestEmitter last = live;
      c.onSample(sample(1L));
      await("live subscriber receives samples", () -> last.received("event:queues"));

      last.fireCompletion();
      assertThat(c.emittersFor(1L)).isEmpty();
      verify(reg, times(n + 1)).unwatch(eq(1L), anyString());
    } finally {
      closer.shutdownNow();
      c.shutdown();
    }
  }
}
