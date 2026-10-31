package com.tsa.mqmonitor.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.atLeastOnce;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.tsa.mqmonitor.client.MqGateway;
import com.tsa.mqmonitor.client.MqGatewayFactory;
import com.tsa.mqmonitor.config.MqMonitorProperties;
import com.tsa.mqmonitor.enumerable.ClientType;
import com.tsa.mqmonitor.enumerable.TlsMode;
import com.tsa.mqmonitor.model.MqConnection;
import com.tsa.mqmonitor.model.MqTarget;
import com.tsa.mqmonitor.model.QueueManagerInfo;
import com.tsa.mqmonitor.model.Thresholds;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class SamplerRegistryTest {
  static final Instant T0 = Instant.parse("2026-10-30T10:00:00Z");

  MutableClock clock = new MutableClock(T0);
  RecordingTaskScheduler recordingTaskScheduler = new RecordingTaskScheduler(clock);
  ConnectionService connectionService = mock(ConnectionService.class);
  MqGatewayFactory mqGatewayFactory = mock(MqGatewayFactory.class);
  MqGateway mqGateway = mock(MqGateway.class);
  Map<Long, MqConnection> connections = new TreeMap<>();
  SamplerRegistry samplerRegistry;

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
    when(connectionService.findById(anyLong()))
        .thenAnswer(inv -> Optional.ofNullable(connections.get(inv.<Long>getArgument(0))));
    when(connectionService.findAll()).thenAnswer(inv -> List.copyOf(connections.values()));
    when(connectionService.findPrometheusExports())
        .thenAnswer(
            inv -> connections.values().stream().filter(MqConnection::prometheusExport).toList());
    when(connectionService.targetFor(any(MqConnection.class))).thenReturn(mock(MqTarget.class));
    when(connectionService.thresholdsFor(any())).thenReturn(new Thresholds(null, 80, 60, null));
    when(mqGatewayFactory.open(any())).thenReturn(mqGateway);
    when(mqGateway.inquireQueueManager()).thenReturn(new QueueManagerInfo("QM", "RUNNING", 940, "UNIX"));
    when(mqGateway.inquireQueues(any(), anyBoolean())).thenReturn(List.of());
    when(mqGateway.inquireChannels()).thenReturn(List.of());
    var props =
        new MqMonitorProperties(
            null,
            "key",
            null,
            new MqMonitorProperties.Sampling(30, 30),
            new MqMonitorProperties.Thresholds(null, 80, 60, null),
            new MqMonitorProperties.Mq(10));
    samplerRegistry = new SamplerRegistry(connectionService, mqGatewayFactory, new HealthEvaluator(), recordingTaskScheduler, clock, props);
  }

  @Test
  void watchStartsSamplingAtConnectionInterval() {
    samplerRegistry.watch(1);
    assertThat(recordingTaskScheduler.periodicTasks()).hasSize(1);
    assertThat(recordingTaskScheduler.period(0)).isEqualTo(Duration.ofSeconds(5));
  }

  @Test
  void secondWatcherDoesNotStartSecondSampler() {
    samplerRegistry.watch(1);
    samplerRegistry.watch(1);
    assertThat(recordingTaskScheduler.periodicTasks()).hasSize(1);
  }

  @Test
  void samplerStopsGraceAfterLastWatcherLeaves() {
    var w = samplerRegistry.watch(1);
    samplerRegistry.unwatch(1, w);
    assertThat(recordingTaskScheduler.periodicTasks()).hasSize(1);
    recordingTaskScheduler.advance(Duration.ofSeconds(30));
    assertThat(recordingTaskScheduler.periodicTasks()).isEmpty();
  }

  @Test
  void watcherReturningWithinGraceCancelsStop() {
    var w = samplerRegistry.watch(1);
    samplerRegistry.unwatch(1, w);
    recordingTaskScheduler.advance(Duration.ofSeconds(29));
    samplerRegistry.watch(1);
    recordingTaskScheduler.advance(Duration.ofSeconds(5));
    assertThat(recordingTaskScheduler.periodicTasks()).hasSize(1);
  }

  @Test
  void prometheusExportSamplesWithoutWatchers() {
    exportOn(2);
    samplerRegistry.refreshPrometheusExports();
    assertThat(recordingTaskScheduler.periodicTasks()).hasSize(1);
  }

  @Test
  void prometheusExportIgnoresUnwatch() {
    exportOn(2);
    samplerRegistry.refreshPrometheusExports();
    var w = samplerRegistry.watch(2);
    samplerRegistry.unwatch(2, w);
    recordingTaskScheduler.advance(Duration.ofMinutes(5));
    assertThat(recordingTaskScheduler.periodicTasks()).hasSize(1);
  }

  @Test
  void listWatchSamplesQmgrOnlyAtListInterval() {
    samplerRegistry.watchList("w");
    recordingTaskScheduler.fireAll();
    verify(mqGateway, atLeastOnce()).inquireQueueManager();
    verify(mqGateway, never()).inquireQueues(any(), anyBoolean());
    assertThat(recordingTaskScheduler.period(0)).isEqualTo(Duration.ofSeconds(30));
  }

  @Test
  void latestIsEmptyBeforeFirstSample() {
    assertThat(samplerRegistry.latest(1)).isEmpty();
  }

  @Test
  void connectionDeletedStopsSampler() {
    samplerRegistry.watch(1);
    samplerRegistry.connectionDeleted(1);
    assertThat(recordingTaskScheduler.periodicTasks()).isEmpty();
  }

  @Test
  void listWatchSkipsConnectionsWithFullSampler() {
    samplerRegistry.watch(1);
    samplerRegistry.watchList("w");
    recordingTaskScheduler.fireAll();
    verify(mqGateway, times(1)).inquireQueues(any(), anyBoolean());
    verify(mqGateway, times(2)).inquireQueueManager();
  }

  @Test
  void listStopsWhenLastListWatcherLeaves() {
    samplerRegistry.watchList("a");
    samplerRegistry.watchList("b");
    assertThat(recordingTaskScheduler.periodicTasks()).hasSize(1);
    samplerRegistry.unwatchList("a");
    assertThat(recordingTaskScheduler.periodicTasks()).hasSize(1);
    samplerRegistry.unwatchList("b");
    assertThat(recordingTaskScheduler.periodicTasks()).isEmpty();
  }

  @Test
  void connectionChangedRestartsAtNewInterval() {
    samplerRegistry.watch(1);
    connections.put(1L, connection(1, 20, false));
    samplerRegistry.connectionChanged(1);
    assertThat(recordingTaskScheduler.periodicTasks()).hasSize(1);
    assertThat(recordingTaskScheduler.period(0)).isEqualTo(Duration.ofSeconds(20));
  }

  @Test
  void connectionChangedWithoutEntryStartsOnlyWhenExportOn() {
    samplerRegistry.connectionChanged(1);
    assertThat(recordingTaskScheduler.periodicTasks()).isEmpty();
    exportOn(1);
    samplerRegistry.connectionChanged(1);
    assertThat(recordingTaskScheduler.periodicTasks()).hasSize(1);
  }

  @Test
  void exportTurnedOffStopsAfterGraceWhenUnwatched() {
    exportOn(2);
    samplerRegistry.refreshPrometheusExports();
    connections.put(2L, connection(2, 10, false));
    samplerRegistry.connectionChanged(2);
    assertThat(recordingTaskScheduler.periodicTasks()).hasSize(1);
    recordingTaskScheduler.advance(Duration.ofSeconds(30));
    assertThat(recordingTaskScheduler.periodicTasks()).isEmpty();
  }

  @Test
  void refreshStopsSamplerThatNoLongerExports() {
    exportOn(2);
    samplerRegistry.refreshPrometheusExports();
    connections.put(2L, connection(2, 10, false));
    samplerRegistry.refreshPrometheusExports();
    assertThat(recordingTaskScheduler.periodicTasks()).isEmpty();
  }

  @Test
  void stopClosesGatewayButKeepsLatest() {
    var w = samplerRegistry.watch(1);
    recordingTaskScheduler.advance(Duration.ofSeconds(1));
    samplerRegistry.unwatch(1, w);
    recordingTaskScheduler.advance(Duration.ofSeconds(30));
    verify(mqGateway, timeout(2000)).close();
    assertThat(samplerRegistry.latest(1)).isPresent();
  }

  @Test
  void graceStopThenWatchReopensOnce() {
    var w = samplerRegistry.watch(1);
    recordingTaskScheduler.advance(Duration.ofSeconds(1));
    samplerRegistry.unwatch(1, w);
    recordingTaskScheduler.advance(Duration.ofSeconds(30));
    verify(mqGateway, timeout(2000).times(1)).close();
    samplerRegistry.watch(1);
    recordingTaskScheduler.advance(Duration.ofSeconds(11));
    verify(mqGatewayFactory, times(2)).open(any());
    assertThat(recordingTaskScheduler.periodicTasks()).hasSize(1);
  }

  @Test
  void runStartedBeforeStopDoesNotReopenGateway() {
    var w = samplerRegistry.watch(1);
    Runnable lateRun = recordingTaskScheduler.periodicTasks().get(0).runnable;
    recordingTaskScheduler.advance(Duration.ofSeconds(1));
    samplerRegistry.unwatch(1, w);
    recordingTaskScheduler.advance(Duration.ofSeconds(30));
    lateRun.run();
    verify(mqGatewayFactory, times(1)).open(any());
    verify(mqGateway, timeout(2000).times(1)).close();
  }

  @Test
  void listPathRetiresSamplerReplacedDuringRun() {
    var swapped = new AtomicBoolean();
    when(mqGateway.inquireQueueManager())
        .thenAnswer(
            inv -> {
              if (swapped.compareAndSet(false, true)) {
                connections.put(2L, connection(2, 15, false));
                samplerRegistry.connectionChanged(2);
              }
              return new QueueManagerInfo("QM", "RUNNING", 940, "UNIX");
            });
    samplerRegistry.watchList("w");
    recordingTaskScheduler.fireAll();
    verify(mqGatewayFactory, times(1)).open(any());
    assertThat(samplerRegistry.latest(2)).isEmpty();
    samplerRegistry.unwatchList("w");
    verify(mqGateway, timeout(2000).times(1)).close();
  }

  @Test
  void listRunDoesNotResurrectDeletedConnection() {
    when(connectionService.findAll())
        .thenAnswer(
            inv -> {
              var snapshot = List.copyOf(connections.values());
              connections.remove(2L);
              samplerRegistry.connectionDeleted(2);
              return snapshot;
            });
    samplerRegistry.watchList("w");
    recordingTaskScheduler.fireAll();
    assertThat(samplerRegistry.latest(2)).isEmpty();
    assertThat(samplerRegistry.latest(1)).isPresent();
    verify(mqGatewayFactory, times(1)).open(any());
    verify(connectionService, times(1)).targetFor(any(MqConnection.class));
  }

  @Test
  void connectionDeletedNotifiesLifecycleListeners() {
    var removed = new java.util.ArrayList<Long>();
    samplerRegistry.addLifecycleListener(removed::add);
    connections.remove(1L);
    samplerRegistry.connectionDeleted(1);
    assertThat(removed).containsExactly(1L);
  }

  @Test
  void exportTurnedOffNotifiesLifecycleListeners() {
    var removed = new java.util.ArrayList<Long>();
    samplerRegistry.addLifecycleListener(removed::add);
    exportOn(1);
    samplerRegistry.connectionChanged(1);
    assertThat(removed).isEmpty();
    connections.put(1L, connection(1, 5, false));
    samplerRegistry.connectionChanged(1);
    assertThat(removed).containsExactly(1L);
  }

  @Test
  void watchdogPushesStaleWhenNoSampleForThreeIntervals() {
    SampleListener sampleListener = mock(SampleListener.class);
    samplerRegistry.addListener(sampleListener);
    samplerRegistry.watch(1); // interval 5s
    recordingTaskScheduler.advance(Duration.ofSeconds(1)); // first sample at T0
    // The periodic task stops producing samples, as a hung sample would.
    recordingTaskScheduler.periodicTasks().get(0).cancel(false);
    recordingTaskScheduler.advance(Duration.ofSeconds(14)); // T0+15s: exactly 3 intervals, not yet stale
    verify(sampleListener, never()).onStale(any());
    recordingTaskScheduler.advance(Duration.ofSeconds(5)); // T0+20s
    verify(sampleListener, times(1)).onStale(any());
    assertThat(samplerRegistry.isStale(1, samplerRegistry.latest(1).orElseThrow())).isTrue();
    recordingTaskScheduler.advance(Duration.ofSeconds(5)); // the push repeats every interval (heartbeat)
    verify(sampleListener, times(2)).onStale(any());
  }

  @Test
  void freshSamplesNeverTriggerStale() {
    SampleListener sampleListener = mock(SampleListener.class);
    samplerRegistry.addListener(sampleListener);
    samplerRegistry.watch(1);
    recordingTaskScheduler.advance(Duration.ofMinutes(2));
    verify(sampleListener, never()).onStale(any());
    assertThat(samplerRegistry.isStale(1, samplerRegistry.latest(1).orElseThrow())).isFalse();
  }

  @Test
  void stoppedEntryHasNoWatchdogButRetainedSampleIsStale() {
    SampleListener sampleListener = mock(SampleListener.class);
    samplerRegistry.addListener(sampleListener);
    var w = samplerRegistry.watch(1);
    recordingTaskScheduler.advance(Duration.ofSeconds(1));
    samplerRegistry.unwatch(1, w);
    recordingTaskScheduler.advance(Duration.ofSeconds(30)); // grace stop
    recordingTaskScheduler.advance(Duration.ofMinutes(5));
    verify(sampleListener, never()).onStale(any());
    assertThat(samplerRegistry.isStale(1, samplerRegistry.latest(1).orElseThrow())).isTrue();
  }
}
