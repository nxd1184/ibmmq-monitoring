package com.tsa.mqmonitor.controller;

import com.tsa.mqmonitor.model.EvaluatedSample;
import com.tsa.mqmonitor.model.HealthState;
import com.tsa.mqmonitor.model.HealthVerdict;
import com.tsa.mqmonitor.model.MqConnection;
import com.tsa.mqmonitor.model.QueueInfo;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Set;
import org.springframework.stereotype.Component;
import org.thymeleaf.TemplateSpec;
import org.thymeleaf.context.Context;
import org.thymeleaf.spring6.SpringTemplateEngine;
import org.thymeleaf.templatemode.TemplateMode;

/**
 * Renders the dashboard and list-row fragments to HTML strings so the same markup serves the
 * first page paint and every SSE push. Runs outside any request (on sampler threads), so the
 * templates use literal URLs rather than context-relative link expressions.
 */
@Component
public class SampleFragmentRenderer {
  static final Set<String> FRAGMENTS = Set.of("qmgr", "queues", "channels");
  private static final Set<String> SELECTOR = Set.of("content");
  private static final Format FORMAT = new Format();

  private final SpringTemplateEngine engine;

  public SampleFragmentRenderer(SpringTemplateEngine engine) {
    this.engine = engine;
  }

  public String render(String fragment, EvaluatedSample s, boolean showSystem) {
    return render(fragment, s, showSystem, false);
  }

  /** {@code stale} switches the qmgr header to "Stale · last sample <time>". */
  public String render(String fragment, EvaluatedSample s, boolean showSystem, boolean stale) {
    if (!FRAGMENTS.contains(fragment)) {
      throw new IllegalArgumentException("Unknown fragment " + fragment);
    }
    Context ctx = new Context();
    ctx.setVariable("sample", s);
    ctx.setVariable("showSystem", showSystem);
    ctx.setVariable("stale", stale);
    ctx.setVariable("queues", visibleQueues(s.sample().queues(), showSystem));
    return process(fragment, ctx);
  }

  public String renderRow(MqConnection c, EvaluatedSample sOrNull) {
    Context ctx = new Context();
    ctx.setVariable("connection", c);
    ctx.setVariable("sample", sOrNull);
    ctx.setVariable("showSystem", false);
    return process("connection-row", ctx);
  }

  private String process(String fragment, Context ctx) {
    ctx.setVariable("fmt", FORMAT);
    return engine.process(
        new TemplateSpec("fragments/" + fragment, SELECTOR, TemplateMode.HTML, null), ctx);
  }

  private static List<QueueInfo> visibleQueues(List<QueueInfo> queues, boolean showSystem) {
    if (queues == null) {
      return List.of();
    }
    return showSystem
        ? queues
        : queues.stream().filter(q -> !q.name().startsWith("SYSTEM.")).toList();
  }

  /** Display formatting used by the fragment templates; {@code n/a} stands in for null. */
  public static final class Format {
    static final String NA = "n/a";

    public String num(Integer v) {
      return v == null ? NA : Integer.toString(v);
    }

    public String age(Integer seconds) {
      return seconds == null ? NA : seconds + "s";
    }

    public String time(LocalDateTime t) {
      return t == null
          ? NA
          : t.truncatedTo(ChronoUnit.SECONDS).format(DateTimeFormatter.ISO_LOCAL_DATE_TIME);
    }

    public String utc(Instant t) {
      return t == null ? NA : time(LocalDateTime.ofInstant(t, ZoneOffset.UTC)) + "Z";
    }

    public String text(Object v) {
      return v == null || v.toString().isBlank() ? NA : v.toString();
    }

    public String healthClass(HealthVerdict v) {
      if (v == null) {
        return "health-unknown";
      }
      return v.state() == HealthState.HEALTHY ? "health-healthy" : "health-unhealthy";
    }

    public String healthText(HealthVerdict v) {
      if (v == null) {
        return NA;
      }
      return v.state() == HealthState.HEALTHY ? "healthy" : "unhealthy";
    }

    public String reason(HealthVerdict v) {
      return v == null ? NA : v.reason();
    }
  }
}
