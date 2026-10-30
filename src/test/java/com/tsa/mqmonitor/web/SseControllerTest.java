package com.tsa.mqmonitor.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
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
import com.tsa.mqmonitor.support.PostgresTestBase;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
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
    finish(result, onlyEmitter(2L));

    String body = result.getResponse().getContentAsString();
    assertThat(body).contains("event:queues").contains("12345");
    verify(registry).unwatch(2L, "w2");
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
    when(registry.watch(99L)).thenThrow(new IllegalArgumentException("Unknown connection 99"));
    mvc.perform(get("/connections/99/stream").with(user("admin")))
        .andExpect(status().isNotFound());
    assertThat(controller.emittersFor(99L)).isEmpty();
  }

  @Test
  void streamRequiresLogin() throws Exception {
    mvc.perform(get("/connections/1/stream")).andExpect(status().is3xxRedirection());
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
}
