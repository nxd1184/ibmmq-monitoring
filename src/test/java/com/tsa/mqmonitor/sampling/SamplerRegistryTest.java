package com.tsa.mqmonitor.sampling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.tsa.mqmonitor.config.MqMonitorProperties;
import com.tsa.mqmonitor.connection.ClientType;
import com.tsa.mqmonitor.connection.ConnectionService;
import com.tsa.mqmonitor.connection.MqConnection;
import com.tsa.mqmonitor.connection.TlsMode;
import com.tsa.mqmonitor.health.HealthEvaluator;
import com.tsa.mqmonitor.health.Thresholds;
import com.tsa.mqmonitor.mq.MqGateway;
import com.tsa.mqmonitor.mq.MqGatewayFactory;
import com.tsa.mqmonitor.mq.MqTarget;
import com.tsa.mqmonitor.mq.QueueManagerInfo;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SamplerRegistryTest {
  static final Instant T0 = Instant.parse("2026-10-30T10:00:00Z");

  MutableClock clock = new MutableClock(T0);
  RecordingTaskScheduler scheduler = new RecordingTaskScheduler(clock);
  ConnectionService service = mock(ConnectionService.class);
  MqGatewayFactory factory = mock(MqGatewayFactory.class);
  MqGateway gw = mock(MqGateway.class);
  Map<Long, MqConnection> connections = new TreeMap<>();
  SamplerRegistry reg;

  static MqConnection connection(long id, int intervalSeconds, boolean export) {
    return new MqConnection(
        id, "QM" + id, "mq", 1414, "QM" + id, "DEV.ADMIN.SVRCONN", "admin", "enc",
        ClientType.NATIVE, TlsMode.NONE, null, null, null, null,
        "*", true, intervalSeconds, export, "SYSTEM.DEFAULT.MODEL.QUEUE",
        null, null, null, null, T0, T0);
  }

  void exportOn(long id) {
    var c = connections.get(id);
    connections.put(id, connection(id, c.sampleIntervalSeconds(), true));
  }

  @BeforeEach
  void setUp() {
    connections.put(1L, connection(1, 5, false));
    connections.put(2L, connection(2, 10, false));
    when(service.findById(anyLong()))
        .thenAnswer(inv -> Optional.ofNullable(connections.get(inv.<Long>getArgument(0))));
    when(service.findAll()).thenAnswer(inv -> List.copyOf(connections.values()));
    when(service.findPrometheusExports())
        .thenAnswer(
            inv -> connections.values().stream().filter(MqConnection::prometheusExport).toList());
    when(service.targetFor(any(MqConnection.class))).thenReturn(mock(MqTarget.class));
    when(service.thresholdsFor(any())).thenReturn(new Thresholds(null, 80, 60, null));
    when(factory.open(any())).thenReturn(gw);
    when(gw.inquireQueueManager()).thenReturn(new QueueManagerInfo("QM", "RUNNING", 940, "UNIX"));
    when(gw.inquireQueues(any(), anyBoolean())).thenReturn(List.of());
    when(gw.inquireChannels()).thenReturn(List.of());
    var props =
        new MqMonitorProperties(
            null,
            "key",
            null,
            new MqMonitorProperties.Sampling(30, 30),
            new MqMonitorProperties.Thresholds(null, 80, 60, null),
            new MqMonitorProperties.Mq(10));
    reg = new SamplerRegistry(service, factory, new HealthEvaluator(), scheduler, clock, props);
  }

  @Test
  void watchStartsSamplingAtConnectionInterval() {
    reg.watch(1);
    assertThat(scheduler.periodicTasks()).hasSize(1);
    assertThat(scheduler.period(0)).isEqualTo(Duration.ofSeconds(5));
  }

  @Test
  void secondWatcherDoesNotStartSecondSampler() {
    reg.watch(1);
    reg.watch(1);
    assertThat(scheduler.periodicTasks()).hasSize(1);
  }

  @Test
  void samplerStopsGraceAfterLastWatcherLeaves() {
    var w = reg.watch(1);
    reg.unwatch(1, w);
    assertThat(scheduler.periodicTasks()).hasSize(1);
    scheduler.advance(Duration.ofSeconds(30));
    assertThat(scheduler.periodicTasks()).isEmpty();
  }

  @Test
  void watcherReturningWithinGraceCancelsStop() {
    var w = reg.watch(1);
    reg.unwatch(1, w);
    scheduler.advance(Duration.ofSeconds(29));
    reg.watch(1);
    scheduler.advance(Duration.ofSeconds(5));
    assertThat(scheduler.periodicTasks()).hasSize(1);
  }

  @Test
  void prometheusExportSamplesWithoutWatchers() {
    exportOn(2);
    reg.refreshPrometheusExports();
    assertThat(scheduler.periodicTasks()).hasSize(1);
  }

  @Test
  void prometheusExportIgnoresUnwatch() {
    exportOn(2);
    reg.refreshPrometheusExports();
    var w = reg.watch(2);
    reg.unwatch(2, w);
    scheduler.advance(Duration.ofMinutes(5));
    assertThat(scheduler.periodicTasks()).hasSize(1);
  }

  @Test
  void listWatchSamplesQmgrOnlyAtListInterval() {
    reg.watchList("w");
    scheduler.fireAll();
    verify(gw, atLeastOnce()).inquireQueueManager();
    verify(gw, never()).inquireQueues(any(), anyBoolean());
    assertThat(scheduler.period(0)).isEqualTo(Duration.ofSeconds(30));
  }

  @Test
  void latestIsEmptyBeforeFirstSample() {
    assertThat(reg.latest(1)).isEmpty();
  }

  @Test
  void connectionDeletedStopsSampler() {
    reg.watch(1);
    reg.connectionDeleted(1);
    assertThat(scheduler.periodicTasks()).isEmpty();
  }

  @Test
  void listWatchSkipsConnectionsWithFullSampler() {
    reg.watch(1);
    reg.watchList("w");
    scheduler.fireAll();
    verify(gw, times(1)).inquireQueues(any(), anyBoolean());
    verify(gw, times(2)).inquireQueueManager();
  }

  @Test
  void listStopsWhenLastListWatcherLeaves() {
    reg.watchList("a");
    reg.watchList("b");
    assertThat(scheduler.periodicTasks()).hasSize(1);
    reg.unwatchList("a");
    assertThat(scheduler.periodicTasks()).hasSize(1);
    reg.unwatchList("b");
    assertThat(scheduler.periodicTasks()).isEmpty();
  }

  @Test
  void connectionChangedRestartsAtNewInterval() {
    reg.watch(1);
    connections.put(1L, connection(1, 20, false));
    reg.connectionChanged(1);
    assertThat(scheduler.periodicTasks()).hasSize(1);
    assertThat(scheduler.period(0)).isEqualTo(Duration.ofSeconds(20));
  }

  @Test
  void connectionChangedWithoutEntryStartsOnlyWhenExportOn() {
    reg.connectionChanged(1);
    assertThat(scheduler.periodicTasks()).isEmpty();
    exportOn(1);
    reg.connectionChanged(1);
    assertThat(scheduler.periodicTasks()).hasSize(1);
  }

  @Test
  void exportTurnedOffStopsAfterGraceWhenUnwatched() {
    exportOn(2);
    reg.refreshPrometheusExports();
    connections.put(2L, connection(2, 10, false));
    reg.connectionChanged(2);
    assertThat(scheduler.periodicTasks()).hasSize(1);
    scheduler.advance(Duration.ofSeconds(30));
    assertThat(scheduler.periodicTasks()).isEmpty();
  }

  @Test
  void refreshStopsSamplerThatNoLongerExports() {
    exportOn(2);
    reg.refreshPrometheusExports();
    connections.put(2L, connection(2, 10, false));
    reg.refreshPrometheusExports();
    assertThat(scheduler.periodicTasks()).isEmpty();
  }

  @Test
  void stopClosesGatewayButKeepsLatest() {
    var w = reg.watch(1);
    scheduler.advance(Duration.ofSeconds(1));
    reg.unwatch(1, w);
    scheduler.advance(Duration.ofSeconds(30));
    verify(gw).close();
    assertThat(reg.latest(1)).isPresent();
  }
}
