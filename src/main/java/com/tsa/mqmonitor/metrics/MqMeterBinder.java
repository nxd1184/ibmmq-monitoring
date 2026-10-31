package com.tsa.mqmonitor.metrics;

import com.tsa.mqmonitor.model.EvaluatedSample;
import com.tsa.mqmonitor.model.HealthState;
import com.tsa.mqmonitor.model.HealthVerdict;
import com.tsa.mqmonitor.model.MqConnection;
import com.tsa.mqmonitor.model.Sample;
import com.tsa.mqmonitor.service.ConnectionLifecycleListener;
import com.tsa.mqmonitor.service.SampleListener;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge.Row;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;

/**
 * Publishes {@code ibmmq.*} gauges for connections that have Prometheus Export on. Each metric is
 * one {@link MultiGauge} rebuilt from every exported connection's rows, so rows that vanish from a
 * sample (or whose connection stops exporting) disappear from the registry.
 */
public class MqMeterBinder implements SampleListener, ConnectionLifecycleListener {
  static final String REACHABLE = "ibmmq.qmgr.reachable";
  static final String DEPTH = "ibmmq.queue.depth";
  static final String MAX_DEPTH = "ibmmq.queue.max.depth";
  static final String OPEN_INPUT = "ibmmq.queue.open.input.count";
  static final String OPEN_OUTPUT = "ibmmq.queue.open.output.count";
  static final String OLDEST_AGE = "ibmmq.queue.oldest.message.age.seconds";
  static final String CHANNEL_STATUS = "ibmmq.channel.status";
  static final String HEALTH = "ibmmq.health";

  private static final List<String> METRICS =
      List.of(
          REACHABLE, DEPTH, MAX_DEPTH, OPEN_INPUT, OPEN_OUTPUT, OLDEST_AGE, CHANNEL_STATUS,
          HEALTH);

  private final Function<Long, Optional<MqConnection>> connectionLookup;
  private final Map<String, MultiGauge> gauges = new HashMap<>();
  private final Map<Long, Map<String, List<Row<?>>>> rowsByConnection = new LinkedHashMap<>();

  public MqMeterBinder(
      MeterRegistry registry, Function<Long, Optional<MqConnection>> connectionLookup) {
    this.connectionLookup = connectionLookup;
    METRICS.forEach(name -> gauges.put(name, MultiGauge.builder(name).register(registry)));
  }

  @Override
  public synchronized void onSample(EvaluatedSample evaluated) {
    Sample sample = evaluated.sample();
    long id = sample.connectionId();
    Optional<MqConnection> found = connectionLookup.apply(id);
    if (found.isEmpty() || !found.get().prometheusExport()) {
      removeRows(id);
      return;
    }
    MqConnection conn = found.get();
    String qmgr =
        sample.qmgr() != null && sample.qmgr().name() != null
            ? sample.qmgr().name()
            : conn.queueManager();
    Tags base = Tags.of("connection", conn.name(), "qmgr", qmgr == null ? "" : qmgr);

    Map<String, List<Row<?>>> rows = new HashMap<>();
    add(rows, REACHABLE, base, sample.reachable() ? 1 : 0);
    if (sample.reachable()) {
      sample.queues().forEach(q -> {
        Tags t = base.and("queue", q.name());
        add(rows, DEPTH, t, q.depth());
        add(rows, MAX_DEPTH, t, q.maxDepth());
        add(rows, OPEN_INPUT, t, q.openInputCount());
        add(rows, OPEN_OUTPUT, t, q.openOutputCount());
        add(rows, OLDEST_AGE, t, q.oldestMessageAgeSeconds());
      });
      sample.channels().forEach(c ->
          add(rows, CHANNEL_STATUS, base.and("channel", c.name()),
              "RUNNING".equals(c.status()) ? 1 : 0));
    }
    addHealth(rows, base, "connection", conn.name(), evaluated.connection());
    if (sample.reachable()) {
      evaluated.queues().forEach((n, v) -> addHealth(rows, base, "queue", n, v));
      evaluated.channels().forEach((n, v) -> addHealth(rows, base, "channel", n, v));
    }
    rowsByConnection.put(id, rows);
    publish();
  }

  @Override
  public synchronized void connectionRemoved(long id) {
    removeRows(id);
  }

  private void removeRows(long id) {
    if (rowsByConnection.remove(id) != null) {
      publish();
    }
  }

  private void publish() {
    for (String metric : METRICS) {
      List<Row<?>> all = new ArrayList<>();
      rowsByConnection.values().forEach(m -> all.addAll(m.getOrDefault(metric, List.of())));
      gauges.get(metric).register(all, true);
    }
  }

  private static void addHealth(
      Map<String, List<Row<?>>> rows, Tags base, String object, String name, HealthVerdict v) {
    if (v == null) {
      return;
    }
    Tags t = base.and("object", object, "name", name, "reason", v.code().name().toLowerCase(Locale.ROOT));
    add(rows, HEALTH, t, v.state() == HealthState.HEALTHY ? 1 : 0);
  }

  private static void add(Map<String, List<Row<?>>> rows, String metric, Tags tags, Number value) {
    if (value != null) {
      rows.computeIfAbsent(metric, k -> new ArrayList<>()).add(Row.of(tags, value));
    }
  }
}
