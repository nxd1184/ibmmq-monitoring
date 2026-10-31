package com.tsa.mqmonitor.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.ibm.mq.headers.pcf.PCFMessage;
import com.tsa.mqmonitor.client.MqGateway;
import com.tsa.mqmonitor.client.MqGatewayFactory;
import com.tsa.mqmonitor.client.PcfMqGateway;
import com.tsa.mqmonitor.client.PcfTransport;
import com.tsa.mqmonitor.enumerable.ClientType;
import com.tsa.mqmonitor.enumerable.HealthState;
import com.tsa.mqmonitor.enumerable.TlsMode;
import com.tsa.mqmonitor.exception.MqUnreachableException;
import com.tsa.mqmonitor.model.EvaluatedSample;
import com.tsa.mqmonitor.model.MqConnection;
import com.tsa.mqmonitor.model.MqTarget;
import com.tsa.mqmonitor.model.QueueInfo;
import com.tsa.mqmonitor.model.QueueManagerInfo;
import com.tsa.mqmonitor.model.Thresholds;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SamplerTest {
  static final Instant T0 = Instant.parse("2026-10-30T10:00:00Z");

  MqGatewayFactory mqGatewayFactory = mock(MqGatewayFactory.class);
  MqGateway mqGateway = mock(MqGateway.class);
  SampleListener sampleListener = mock(SampleListener.class);
  MutableClock clock = new MutableClock(T0);
  QueueManagerInfo queueManagerInfo = new QueueManagerInfo("QM1", "RUNNING", 940, "UNIX");
  Sampler sampler;

  static MqConnection connection() {
    return new MqConnection(
        1L, "QM1", "mq", 1414, "QM1", "DEV.ADMIN.SVRCONN", "admin", "enc",
        ClientType.NATIVE, TlsMode.NONE, null, null, null, null,
        "APP.*", true, 5, false, "SYSTEM.DEFAULT.MODEL.QUEUE",
        null, null, null, null, T0, T0);
  }

  static QueueInfo q(int in) {
    return new QueueInfo("APP.Q", 0, 5000, in, 1, null, null, null);
  }

  @BeforeEach
  void setUp() {
    when(mqGatewayFactory.open(any())).thenReturn(mqGateway);
    when(mqGateway.inquireQueueManager()).thenReturn(queueManagerInfo);
    when(mqGateway.inquireQueues(any(), anyBoolean())).thenReturn(List.of());
    when(mqGateway.inquireChannels()).thenReturn(List.of());
    sampler =
        new Sampler(
            connection(),
            mock(MqTarget.class),
            new Thresholds(null, 80, 60, null),
            mqGatewayFactory,
            new HealthEvaluator(),
            clock,
            List.of(sampleListener));
  }

  @Test
  void reusesOneGatewayAcrossSamples() {
    sampler.sampleOnce();
    sampler.sampleOnce();
    verify(mqGatewayFactory, times(1)).open(any());
  }

  @Test
  void failureProducesFailedSampleAndReconnectsNextTime() {
    when(mqGateway.inquireQueueManager())
        .thenThrow(new MqUnreachableException("MQRC 2009 connection broken", null))
        .thenReturn(queueManagerInfo);
    assertThat(sampler.sampleOnce().sample().error()).isEqualTo("MQRC 2009 connection broken");
    sampler.sampleOnce();
    verify(mqGatewayFactory, times(2)).open(any());
    verify(mqGateway, timeout(2000)).close();
  }

  @Test
  void noConsumerSinceIsTrackedAcrossSamples() {
    clock.set(T0);
    when(mqGateway.inquireQueues(any(), anyBoolean())).thenReturn(List.of(q(0)));
    sampler.sampleOnce();
    clock.set(T0.plusSeconds(61));
    assertThat(sampler.sampleOnce().queues().get("APP.Q").reason()).isEqualTo("no consumer for 61s");
  }

  @Test
  void consumerReturningResetsTracking() {
    when(mqGateway.inquireQueues(any(), anyBoolean()))
        .thenReturn(List.of(q(0)))
        .thenReturn(List.of(q(1)))
        .thenReturn(List.of(q(0)));
    sampler.sampleOnce();
    clock.set(T0.plusSeconds(61));
    sampler.sampleOnce();
    clock.set(T0.plusSeconds(122));
    assertThat(sampler.sampleOnce().queues().get("APP.Q").state()).isEqualTo(HealthState.HEALTHY);
  }

  @Test
  void listenersReceiveEverySample() {
    sampler.sampleOnce();
    verify(sampleListener).onSample(any());
  }

  @Test
  void failingListenerDoesNotStopOthers() {
    SampleListener second = mock(SampleListener.class);
    SampleListener failing = mock(SampleListener.class);
    doThrow(new IllegalStateException("boom")).when(failing).onSample(any());
    var s =
        new Sampler(
            connection(), mock(MqTarget.class), new Thresholds(null, 80, 60, null), mqGatewayFactory,
            new HealthEvaluator(), clock, List.of(failing, second));
    s.sampleOnce();
    verify(second).onSample(any());
  }

  @Test
  void queueManagerOnlySampleSkipsQueuesAndChannels() {
    var e = sampler.sampleQueueManagerOnly();
    assertThat(e.sample().qmgr()).isEqualTo(queueManagerInfo);
    assertThat(e.sample().queues()).isEmpty();
    verify(mqGateway, org.mockito.Mockito.never()).inquireQueues(any(), anyBoolean());
    verify(mqGateway, org.mockito.Mockito.never()).inquireChannels();
  }

  @Test
  void retiredSamplerNeverReopensGateway() {
    sampler.sampleOnce();
    sampler.retire();
    sampler.sampleOnce();
    sampler.sampleQueueManagerOnly();
    verify(mqGatewayFactory, times(1)).open(any());
    verify(mqGateway, timeout(2000)).close();
  }

  /** A transport whose send never returns, like a PCF exchange with a frozen queue manager. */
  static final class BlockingTransport implements PcfTransport {
    final CountDownLatch sending = new CountDownLatch(1);
    final CountDownLatch closed = new CountDownLatch(1);

    @Override
    public List<PCFMessage> send(PCFMessage request) {
      sending.countDown();
      while (true) {
        try {
          new CountDownLatch(1).await(); // ignores interrupts, as a blocked socket read does
        } catch (InterruptedException e) {
          // keep blocking
        }
      }
    }

    @Override
    public void close() {
      closed.countDown();
    }
  }

  @Test
  void frozenQueueManagerFailsTheSampleWithinTheBoundAndReconnectsNextTime() throws Exception {
    var frozen = new BlockingTransport();
    when(mqGatewayFactory.open(any()))
        .thenReturn(new PcfMqGateway(frozen, Duration.ofMillis(300)))
        .thenReturn(mqGateway);
    long start = System.nanoTime();
    EvaluatedSample failed = sampler.sampleOnce();
    long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

    assertThat(failed.sample().reachable()).isFalse();
    assertThat(failed.sample().error()).isEqualTo("no PCF reply within 300ms");
    assertThat(elapsedMs).isLessThan(2_000);
    assertThat(frozen.closed.await(2, TimeUnit.SECONDS)).as("broken gateway closed").isTrue();

    EvaluatedSample next = sampler.sampleOnce();
    assertThat(next.sample().reachable()).isTrue();
    verify(mqGatewayFactory, times(2)).open(any());
  }

  @Test
  void retireWhileASampleIsBlockedReturnsPromptly() throws Exception {
    var frozen = new BlockingTransport();
    when(mqGatewayFactory.open(any())).thenReturn(new PcfMqGateway(frozen, Duration.ofSeconds(30)));
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      Future<EvaluatedSample> running = pool.submit(sampler::sampleOnce);
      assertThat(frozen.sending.await(2, TimeUnit.SECONDS)).isTrue();

      long start = System.nanoTime();
      sampler.retire();
      sampler.releaseGateway();
      sampler.close();
      assertThat(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)).isLessThan(500);
      assertThat(frozen.closed.await(2, TimeUnit.SECONDS)).as("gateway closed").isTrue();
      assertThat(running.isDone()).isFalse(); // still stuck in the frozen send
    } finally {
      pool.shutdownNow();
    }
  }

  @Test
  void retiredSamplerPublishesNothing() throws Exception {
    var inQmgr = new CountDownLatch(1);
    var proceed = new CountDownLatch(1);
    when(mqGateway.inquireQueueManager())
        .thenAnswer(
            inv -> {
              inQmgr.countDown();
              proceed.await(2, TimeUnit.SECONDS);
              return queueManagerInfo;
            });
    ExecutorService pool = Executors.newSingleThreadExecutor();
    try {
      Future<EvaluatedSample> running = pool.submit(sampler::sampleOnce);
      assertThat(inQmgr.await(2, TimeUnit.SECONDS)).isTrue();
      sampler.retire();
      proceed.countDown();
      running.get(2, TimeUnit.SECONDS);
    } finally {
      pool.shutdownNow();
    }
    verify(sampleListener, never()).onSample(any());
    assertThat(sampler.latest()).isEmpty();
  }
}
