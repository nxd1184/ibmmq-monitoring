package com.tsa.mqmonitor.web;

import static org.assertj.core.api.Assertions.assertThat;

import com.tsa.mqmonitor.connection.ClientType;
import com.tsa.mqmonitor.connection.MqConnection;
import com.tsa.mqmonitor.connection.TlsMode;
import com.tsa.mqmonitor.health.HealthReason;
import com.tsa.mqmonitor.health.HealthVerdict;
import com.tsa.mqmonitor.mq.ChannelInfo;
import com.tsa.mqmonitor.mq.QueueInfo;
import com.tsa.mqmonitor.mq.QueueManagerInfo;
import com.tsa.mqmonitor.sampling.EvaluatedSample;
import com.tsa.mqmonitor.sampling.Sample;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.thymeleaf.spring6.SpringTemplateEngine;
import org.thymeleaf.templatemode.TemplateMode;
import org.thymeleaf.templateresolver.ClassLoaderTemplateResolver;

class SampleFragmentRendererTest {
  private static final Instant AT = Instant.parse("2026-10-30T10:15:30.123Z");

  private final SampleFragmentRenderer renderer = new SampleFragmentRenderer(engine());

  static SpringTemplateEngine engine() {
    var resolver = new ClassLoaderTemplateResolver();
    resolver.setPrefix("templates/");
    resolver.setSuffix(".html");
    resolver.setTemplateMode(TemplateMode.HTML);
    resolver.setCharacterEncoding("UTF-8");
    var engine = new SpringTemplateEngine();
    engine.setTemplateResolver(resolver);
    return engine;
  }

  private static QueueInfo queue(String name, int depth, Integer age) {
    return new QueueInfo(name, depth, 100, 1, 0, age, LocalDateTime.of(2026, 10, 30, 9, 0, 1, 500), null);
  }

  private static EvaluatedSample sample(List<QueueInfo> queues, Map<String, HealthVerdict> verdicts) {
    var s =
        new Sample(
            1L,
            AT,
            new QueueManagerInfo("QM1", "RUNNING", 940, "UNIX"),
            queues,
            List.of(new ChannelInfo("DEV.APP.SVRCONN", "SVRCONN", "RUNNING")),
            null);
    return new EvaluatedSample(
        s,
        HealthVerdict.healthy(),
        verdicts,
        Map.of("DEV.APP.SVRCONN", HealthVerdict.healthy()));
  }

  @Test
  void nullAgeRendersNa() {
    var html =
        renderer.render(
            "queues",
            sample(List.of(queue("DEV.Q1", 3, null)), Map.of("DEV.Q1", HealthVerdict.healthy())),
            false);
    assertThat(html).contains(">n/a<").doesNotContain(">-1<").contains(">3<");
  }

  @Test
  void unhealthyQueueHasClassAndReason() {
    var verdict = HealthVerdict.unhealthy(HealthReason.DEPTH_PERCENT, "depth 80 is 80% of max 100");
    var html =
        renderer.render("queues", sample(List.of(queue("DEV.Q1", 80, 42)), Map.of("DEV.Q1", verdict)), false);
    assertThat(html)
        .contains("class=\"health-unhealthy\"")
        .contains("title=\"depth 80 is 80% of max 100\"")
        .contains(">42s<")
        .contains("2026-10-30T09:00:01");
  }

  @Test
  void systemQueuesHiddenUnlessFlag() {
    var s =
        sample(
            List.of(queue("DEV.Q1", 1, 1), queue("SYSTEM.X", 2, 2)),
            Map.of("DEV.Q1", HealthVerdict.healthy(), "SYSTEM.X", HealthVerdict.healthy()));
    assertThat(renderer.render("queues", s, false)).contains("DEV.Q1").doesNotContain("SYSTEM.X");
    assertThat(renderer.render("queues", s, true)).contains("DEV.Q1").contains("SYSTEM.X");
  }

  @Test
  void authorityErrorRendersObjectName() {
    var failed =
        new EvaluatedSample(
            Sample.failed(1L, AT, "authority missing on DEV.* (MQRC 2035)"),
            HealthVerdict.unhealthy(HealthReason.UNREACHABLE, "authority missing on DEV.* (MQRC 2035)"),
            Map.of(),
            Map.of());
    var html = renderer.render("qmgr", failed, false);
    assertThat(html).contains("authority missing on DEV.*").contains("class=\"health-unhealthy\"");
  }

  @Test
  void qmgrShowsDetailsAndUtcSampleTime() {
    var html = renderer.render("qmgr", sample(List.of(), Map.of()), false);
    assertThat(html)
        .contains("QM1")
        .contains("RUNNING")
        .contains("UNIX")
        .contains(">940<")
        .contains("2026-10-30T10:15:30")
        .doesNotContain("10:15:30.123")
        .contains("class=\"health-healthy\"");
  }

  @Test
  void channelsTableShowsChannel() {
    var html = renderer.render("channels", sample(List.of(), Map.of()), false);
    assertThat(html).contains("DEV.APP.SVRCONN").contains("SVRCONN").contains("health-healthy");
  }

  @Test
  void rowShowsWaitingWithoutSampleAndLinksToDashboard() {
    var c = connection(7L);
    var html = renderer.renderRow(c, null);
    assertThat(html)
        .contains("id=\"conn-7\"")
        .contains("mq.example:1414")
        .contains("waiting")
        .contains("href=\"/connections/7\"");
  }

  @Test
  void rowShowsUnreachableForFailedSample() {
    var failed =
        new EvaluatedSample(
            Sample.failed(7L, AT, "host down"),
            HealthVerdict.unhealthy(HealthReason.UNREACHABLE, "host down"),
            Map.of(),
            Map.of());
    var html = renderer.renderRow(connection(7L), failed);
    assertThat(html).contains("unreachable").contains("title=\"host down\"");
  }

  @Test
  void staleQmgrHeaderShowsLastSampleTimeWithWarningStyle() {
    var s =
        new EvaluatedSample(
            new Sample(1L, AT, new QueueManagerInfo("QM1", "RUNNING", 940, "UNIX"), List.of(), List.of(), null),
            HealthVerdict.healthy(),
            Map.of(),
            Map.of());
    var html = renderer.render("qmgr", s, false, true);
    assertThat(html)
        .contains("Stale · last sample")
        .contains("live-stale")
        .contains("2026-10-30T10:15:30Z")
        .doesNotContain("live-word");
    assertThat(renderer.render("qmgr", s, false)).contains("live-word").doesNotContain("Stale");
  }

  static MqConnection connection(long id) {
    return new MqConnection(
        id, "Prod QM", "mq.example", 1414, "QM1", "DEV.APP.SVRCONN", null, null,
        ClientType.NATIVE, TlsMode.NONE, null, null, null, null, "*", true, 5, false,
        "SYSTEM.DEFAULT.MODEL.QUEUE", null, null, null, null, Instant.now(), Instant.now());
  }
}
