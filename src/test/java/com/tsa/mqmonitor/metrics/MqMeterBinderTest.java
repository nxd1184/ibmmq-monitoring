package com.tsa.mqmonitor.metrics;

import static org.assertj.core.api.Assertions.assertThat;

import com.tsa.mqmonitor.enumerable.ClientType;
import com.tsa.mqmonitor.enumerable.HealthReason;
import com.tsa.mqmonitor.enumerable.TlsMode;
import com.tsa.mqmonitor.model.ChannelInfo;
import com.tsa.mqmonitor.model.EvaluatedSample;
import com.tsa.mqmonitor.model.HealthVerdict;
import com.tsa.mqmonitor.model.MqConnection;
import com.tsa.mqmonitor.model.QueueInfo;
import com.tsa.mqmonitor.model.QueueManagerInfo;
import com.tsa.mqmonitor.model.Sample;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;

class MqMeterBinderTest {
  static final Instant T0 = Instant.parse("2026-10-30T10:00:00Z");

  SimpleMeterRegistry simpleMeterRegistry = new SimpleMeterRegistry();
  Map<Long, MqConnection> connections = new HashMap<>();
  MqMeterBinder mqMeterBinder = new MqMeterBinder(simpleMeterRegistry, id -> Optional.ofNullable(connections.get(id)));

  MqConnection connection(long id, String name, boolean export) {
    var c =
        new MqConnection(
            id, name, "mq", 1414, name, "CH", "admin", "enc", ClientType.NATIVE, TlsMode.NONE,
            null, null, null, null, "*", true, 5, export, "MODEL", null, null, null, null, T0, T0);
    connections.put(id, c);
    return c;
  }

  static QueueInfo queue(String name, int depth, Integer age) {
    return new QueueInfo(name, depth, 100, 1, 2, age, null, null);
  }

  EvaluatedSample sample(
      MqConnection c, List<QueueInfo> queues, List<ChannelInfo> channels,
      Map<String, HealthVerdict> queueVerdicts) {
    var s = new Sample(c.id(), T0, new QueueManagerInfo(c.name(), "RUNNING", 940, "UNIX"),
        queues, channels, null);
    return new EvaluatedSample(s, HealthVerdict.healthy(), queueVerdicts, Map.of());
  }

  @Test
  void publishesDepthForExportedConnection() {
    var c = connection(1, "QM1", true);
    mqMeterBinder.onSample(sample(c, List.of(queue("APP.Q", 7, null)), List.of(), Map.of()));
    assertThat(
            simpleMeterRegistry.get("ibmmq.queue.depth").tags("connection", "QM1", "queue", "APP.Q", "qmgr", "QM1")
                .gauge().value())
        .isEqualTo(7);
    assertThat(simpleMeterRegistry.get("ibmmq.queue.max.depth").gauge().value()).isEqualTo(100);
    assertThat(simpleMeterRegistry.get("ibmmq.queue.open.input.count").gauge().value()).isEqualTo(1);
    assertThat(simpleMeterRegistry.get("ibmmq.queue.open.output.count").gauge().value()).isEqualTo(2);
    assertThat(simpleMeterRegistry.get("ibmmq.qmgr.reachable").gauge().value()).isEqualTo(1);
  }

  @Test
  void ignoresNonExportedConnection() {
    var c = connection(2, "QM2", false);
    mqMeterBinder.onSample(sample(c, List.of(queue("A", 1, null)), List.of(), Map.of()));
    assertThat(simpleMeterRegistry.find("ibmmq.queue.depth").gauge()).isNull();
    assertThat(simpleMeterRegistry.find("ibmmq.qmgr.reachable").gauge()).isNull();
  }

  @Test
  void nullAgeNotPublished() {
    var c = connection(1, "QM1", true);
    mqMeterBinder.onSample(sample(c, List.of(queue("A", 1, null)), List.of(), Map.of()));
    assertThat(simpleMeterRegistry.find("ibmmq.queue.oldest.message.age.seconds").gauge()).isNull();
    mqMeterBinder.onSample(sample(c, List.of(queue("A", 1, 42)), List.of(), Map.of()));
    assertThat(simpleMeterRegistry.get("ibmmq.queue.oldest.message.age.seconds").gauge().value()).isEqualTo(42);
  }

  @Test
  void healthReasonIsATag() {
    var c = connection(1, "QM1", true);
    String reason = "depth 80 is 80% of max 100";
    mqMeterBinder.onSample(
        sample(c, List.of(queue("APP.Q", 80, null)), List.of(),
            Map.of("APP.Q", HealthVerdict.unhealthy(HealthReason.DEPTH_PERCENT, reason))));
    assertThat(
            simpleMeterRegistry.get("ibmmq.health")
                .tags("object", "queue", "name", "APP.Q", "reason", "depth_percent", "connection", "QM1")
                .gauge().value())
        .isEqualTo(0);
    assertThat(simpleMeterRegistry.get("ibmmq.health").tags("object", "connection", "name", "QM1").gauge().value())
        .isEqualTo(1);
  }

  @Test
  void channelStatusIsOneOnlyWhenRunning() {
    var c = connection(1, "QM1", true);
    mqMeterBinder.onSample(
        sample(c, List.of(), List.of(new ChannelInfo("C1", "SVRCONN", "RUNNING"),
            new ChannelInfo("C2", "SDR", "STOPPED")), Map.of()));
    assertThat(simpleMeterRegistry.get("ibmmq.channel.status").tag("channel", "C1").gauge().value()).isEqualTo(1);
    assertThat(simpleMeterRegistry.get("ibmmq.channel.status").tag("channel", "C2").gauge().value()).isEqualTo(0);
  }

  @Test
  void unreachableSamplePublishesZeroReachable() {
    var c = connection(1, "QM1", true);
    mqMeterBinder.onSample(sample(c, List.of(queue("A", 1, 5)), List.of(new ChannelInfo("C", "T", "RUNNING")), Map.of()));
    var failed = new EvaluatedSample(Sample.failed(1, T0, "down"),
        HealthVerdict.unhealthy(HealthReason.UNREACHABLE, "down"), Map.of(), Map.of());
    mqMeterBinder.onSample(failed);
    assertThat(simpleMeterRegistry.get("ibmmq.health").tag("reason", "unreachable").gauge().value()).isEqualTo(0);
    assertThat(simpleMeterRegistry.get("ibmmq.qmgr.reachable").gauge().value()).isEqualTo(0);
    assertThat(simpleMeterRegistry.find("ibmmq.queue.depth").gauge()).isNull();
    assertThat(simpleMeterRegistry.find("ibmmq.channel.status").gauge()).isNull();
  }

  @Test
  void connectionRemovedDropsOnlyItsRows() {
    var a = connection(1, "QM1", true);
    var b = connection(2, "QM2", true);
    mqMeterBinder.onSample(sample(a, List.of(queue("A", 1, null)), List.of(), Map.of()));
    mqMeterBinder.onSample(sample(b, List.of(queue("B", 2, null)), List.of(), Map.of()));
    mqMeterBinder.connectionRemoved(1);
    assertThat(simpleMeterRegistry.find("ibmmq.queue.depth").tag("connection", "QM1").gauge()).isNull();
    assertThat(simpleMeterRegistry.get("ibmmq.queue.depth").tag("connection", "QM2").gauge().value()).isEqualTo(2);
  }

  @Test
  void sampleAfterExportTurnedOffRemovesRows() {
    var c = connection(1, "QM1", true);
    mqMeterBinder.onSample(sample(c, List.of(queue("A", 1, null)), List.of(), Map.of()));
    var off = connection(1, "QM1", false);
    mqMeterBinder.onSample(sample(off, List.of(queue("A", 1, null)), List.of(), Map.of()));
    assertThat(simpleMeterRegistry.find("ibmmq.queue.depth").gauge()).isNull();
  }
}
