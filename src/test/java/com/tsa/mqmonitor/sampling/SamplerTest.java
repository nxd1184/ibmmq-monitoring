package com.tsa.mqmonitor.sampling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.tsa.mqmonitor.connection.ClientType;
import com.tsa.mqmonitor.connection.MqConnection;
import com.tsa.mqmonitor.connection.TlsMode;
import com.tsa.mqmonitor.health.HealthEvaluator;
import com.tsa.mqmonitor.health.HealthState;
import com.tsa.mqmonitor.health.Thresholds;
import com.tsa.mqmonitor.mq.MqGateway;
import com.tsa.mqmonitor.mq.MqGatewayFactory;
import com.tsa.mqmonitor.mq.MqTarget;
import com.tsa.mqmonitor.mq.MqUnreachableException;
import com.tsa.mqmonitor.mq.QueueInfo;
import com.tsa.mqmonitor.mq.QueueManagerInfo;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SamplerTest {
  static final Instant T0 = Instant.parse("2026-10-30T10:00:00Z");

  MqGatewayFactory factory = mock(MqGatewayFactory.class);
  MqGateway gw = mock(MqGateway.class);
  SampleListener listener = mock(SampleListener.class);
  MutableClock clock = new MutableClock(T0);
  QueueManagerInfo qm = new QueueManagerInfo("QM1", "RUNNING", 940, "UNIX");
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
    when(factory.open(any())).thenReturn(gw);
    when(gw.inquireQueueManager()).thenReturn(qm);
    when(gw.inquireQueues(any(), anyBoolean())).thenReturn(List.of());
    when(gw.inquireChannels()).thenReturn(List.of());
    sampler =
        new Sampler(
            connection(),
            mock(MqTarget.class),
            new Thresholds(null, 80, 60, null),
            factory,
            new HealthEvaluator(),
            clock,
            List.of(listener));
  }

  @Test
  void reusesOneGatewayAcrossSamples() {
    sampler.sampleOnce();
    sampler.sampleOnce();
    verify(factory, times(1)).open(any());
  }

  @Test
  void failureProducesFailedSampleAndReconnectsNextTime() {
    when(gw.inquireQueueManager())
        .thenThrow(new MqUnreachableException("MQRC 2009 connection broken", null))
        .thenReturn(qm);
    assertThat(sampler.sampleOnce().sample().error()).isEqualTo("MQRC 2009 connection broken");
    sampler.sampleOnce();
    verify(factory, times(2)).open(any());
    verify(gw).close();
  }

  @Test
  void noConsumerSinceIsTrackedAcrossSamples() {
    clock.set(T0);
    when(gw.inquireQueues(any(), anyBoolean())).thenReturn(List.of(q(0)));
    sampler.sampleOnce();
    clock.set(T0.plusSeconds(61));
    assertThat(sampler.sampleOnce().queues().get("APP.Q").reason()).isEqualTo("no consumer for 61s");
  }

  @Test
  void consumerReturningResetsTracking() {
    when(gw.inquireQueues(any(), anyBoolean()))
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
    verify(listener).onSample(any());
  }

  @Test
  void failingListenerDoesNotStopOthers() {
    SampleListener second = mock(SampleListener.class);
    SampleListener failing = mock(SampleListener.class);
    doThrow(new IllegalStateException("boom")).when(failing).onSample(any());
    var s =
        new Sampler(
            connection(), mock(MqTarget.class), new Thresholds(null, 80, 60, null), factory,
            new HealthEvaluator(), clock, List.of(failing, second));
    s.sampleOnce();
    verify(second).onSample(any());
  }

  @Test
  void queueManagerOnlySampleSkipsQueuesAndChannels() {
    var e = sampler.sampleQueueManagerOnly();
    assertThat(e.sample().qmgr()).isEqualTo(qm);
    assertThat(e.sample().queues()).isEmpty();
    verify(gw, org.mockito.Mockito.never()).inquireQueues(any(), anyBoolean());
    verify(gw, org.mockito.Mockito.never()).inquireChannels();
  }

  @Test
  void retiredSamplerNeverReopensGateway() {
    sampler.sampleOnce();
    sampler.retire();
    sampler.sampleOnce();
    sampler.sampleQueueManagerOnly();
    verify(factory, times(1)).open(any());
    verify(gw).close();
  }
}
