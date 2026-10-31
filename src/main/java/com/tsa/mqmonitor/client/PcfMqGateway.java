package com.tsa.mqmonitor.client;

import com.ibm.mq.MQException;
import com.ibm.mq.constants.CMQC;
import com.ibm.mq.constants.CMQCFC;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.headers.MQDataException;
import com.ibm.mq.headers.pcf.PCFMessage;
import com.tsa.mqmonitor.exception.MqGatewayException;
import com.tsa.mqmonitor.exception.MqUnreachableException;
import com.tsa.mqmonitor.model.ChannelInfo;
import com.tsa.mqmonitor.model.QueueInfo;
import com.tsa.mqmonitor.model.QueueManagerInfo;
import com.tsa.mqmonitor.util.BoundedCall;
import java.io.IOException;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * {@link MqGateway} over a {@link PcfTransport}. Every PCF exchange runs on a daemon thread and is
 * bounded by a per-sample deadline: {@link #beginSample()} starts a budget of {@code timeout} that
 * all exchanges until the next call share (without a call, each exchange gets the full budget). A
 * frozen queue manager therefore fails a sample within the timeout instead of hanging the caller
 * until TCP or the channel heartbeat gives up. On a timeout the gateway is broken: it closes its
 * transport asynchronously and refuses further exchanges, so the caller opens a new one.
 */
public class PcfMqGateway implements MqGateway {
  /** Used by tests and callers that do not supply the connection's timeout. */
  static final Duration DEFAULT_TIMEOUT = Duration.ofSeconds(10);

  private static final String QMGR_OBJECT = "queue manager";
  private static final DateTimeFormatter PUT_DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");
  private static final DateTimeFormatter PUT_TIME = DateTimeFormatter.ofPattern("HH.mm.ss");

  private final PcfTransport transport;
  private final Duration timeout;
  private final AtomicBoolean closed = new AtomicBoolean();
  private volatile boolean broken;
  private volatile long deadlineNanos;
  private volatile boolean deadlineSet;

  public PcfMqGateway(PcfTransport transport) {
    this(transport, DEFAULT_TIMEOUT);
  }

  public PcfMqGateway(PcfTransport transport, Duration timeout) {
    this.transport = transport;
    this.timeout = timeout == null || timeout.isZero() || timeout.isNegative() ? DEFAULT_TIMEOUT : timeout;
  }

  @Override
  public void beginSample() {
    deadlineNanos = System.nanoTime() + timeout.toNanos();
    deadlineSet = true;
  }

  @Override
  public QueueManagerInfo inquireQueueManager() {
    List<PCFMessage> status =
        exchange(new PCFMessage(CMQCFC.MQCMD_INQUIRE_Q_MGR_STATUS), QMGR_OBJECT);
    var attrs = new PCFMessage(CMQCFC.MQCMD_INQUIRE_Q_MGR);
    attrs.addParameter(CMQCFC.MQIACF_Q_MGR_ATTRS, new int[] {CMQC.MQIA_COMMAND_LEVEL, CMQC.MQIA_PLATFORM});
    List<PCFMessage> info = exchange(attrs, QMGR_OBJECT);

    String name = null;
    String state = "UNKNOWN";
    if (!status.isEmpty()) {
      PCFMessage s = status.get(0);
      name = str(s, CMQC.MQCA_Q_MGR_NAME);
      Integer code = integer(s, CMQCFC.MQIACF_Q_MGR_STATUS);
      if (code != null) state = qmgrState(code);
    }
    Integer level = null;
    String platform = null;
    if (!info.isEmpty()) {
      level = integer(info.get(0), CMQC.MQIA_COMMAND_LEVEL);
      Integer p = integer(info.get(0), CMQC.MQIA_PLATFORM);
      if (p != null) platform = platformName(p);
    }
    return new QueueManagerInfo(name, state, level, platform);
  }

  @Override
  public List<QueueInfo> inquireQueues(String filter, boolean hideSystemQueues) {
    var q = new PCFMessage(CMQCFC.MQCMD_INQUIRE_Q);
    q.addParameter(CMQC.MQCA_Q_NAME, filter);
    q.addParameter(CMQC.MQIA_Q_TYPE, CMQC.MQQT_LOCAL);
    q.addParameter(
        CMQCFC.MQIACF_Q_ATTRS,
        new int[] {CMQC.MQCA_Q_NAME, CMQC.MQIA_CURRENT_Q_DEPTH, CMQC.MQIA_MAX_Q_DEPTH});
    // A filter that matches no queue is reported as MQRC_UNKNOWN_OBJECT_NAME.
    List<PCFMessage> queues = exchange(q, filter, CMQC.MQRC_UNKNOWN_OBJECT_NAME);

    var st = new PCFMessage(CMQCFC.MQCMD_INQUIRE_Q_STATUS);
    st.addParameter(CMQC.MQCA_Q_NAME, filter);
    st.addParameter(CMQCFC.MQIACF_Q_STATUS_TYPE, CMQCFC.MQIACF_Q_STATUS);
    st.addParameter(
        CMQCFC.MQIACF_Q_STATUS_ATTRS,
        new int[] {
          CMQC.MQCA_Q_NAME,
          CMQC.MQIA_OPEN_INPUT_COUNT,
          CMQC.MQIA_OPEN_OUTPUT_COUNT,
          CMQCFC.MQIACF_OLDEST_MSG_AGE,
          CMQCFC.MQCACF_LAST_PUT_DATE,
          CMQCFC.MQCACF_LAST_PUT_TIME,
          CMQCFC.MQCACF_LAST_GET_DATE,
          CMQCFC.MQCACF_LAST_GET_TIME
        });
    Map<String, PCFMessage> statusByName = new HashMap<>();
    for (PCFMessage m : exchange(st, filter, CMQC.MQRC_UNKNOWN_OBJECT_NAME)) {
      String name = str(m, CMQC.MQCA_Q_NAME);
      if (name != null) statusByName.put(name, m);
    }

    List<QueueInfo> result = new ArrayList<>();
    for (PCFMessage m : queues) {
      String name = str(m, CMQC.MQCA_Q_NAME);
      if (name == null || name.startsWith("AMQ.")) continue;
      if (hideSystemQueues && name.startsWith("SYSTEM.")) continue;
      PCFMessage s = statusByName.get(name);
      result.add(
          new QueueInfo(
              name,
              requiredInt(m, CMQC.MQIA_CURRENT_Q_DEPTH, name),
              requiredInt(m, CMQC.MQIA_MAX_Q_DEPTH, name),
              s == null ? null : integer(s, CMQC.MQIA_OPEN_INPUT_COUNT),
              s == null ? null : integer(s, CMQC.MQIA_OPEN_OUTPUT_COUNT),
              s == null ? null : age(s),
              s == null ? null : dateTime(s, CMQCFC.MQCACF_LAST_PUT_DATE, CMQCFC.MQCACF_LAST_PUT_TIME),
              s == null ? null : dateTime(s, CMQCFC.MQCACF_LAST_GET_DATE, CMQCFC.MQCACF_LAST_GET_TIME)));
    }
    return result;
  }

  @Override
  public List<ChannelInfo> inquireChannels() {
    var req = new PCFMessage(CMQCFC.MQCMD_INQUIRE_CHANNEL_STATUS);
    req.addParameter(CMQCFC.MQCACH_CHANNEL_NAME, "*");
    req.addParameter(CMQCFC.MQIACH_CHANNEL_INSTANCE_TYPE, CMQC.MQOT_CURRENT_CHANNEL);
    List<ChannelInfo> result = new ArrayList<>();
    for (PCFMessage m : exchange(req, QMGR_OBJECT, CMQCFC.MQRCCF_CHL_STATUS_NOT_FOUND)) {
      String name = str(m, CMQCFC.MQCACH_CHANNEL_NAME);
      if (name == null) continue;
      Integer type = integer(m, CMQCFC.MQIACH_CHANNEL_TYPE);
      Integer status = integer(m, CMQCFC.MQIACH_CHANNEL_STATUS);
      result.add(
          new ChannelInfo(
              name,
              type == null ? null : lookupName(type, "MQCHT_.*", "MQCHT_"),
              status == null ? null : lookupName(status, "MQCHS_.*", "MQCHS_")));
    }
    return result;
  }

  /** Idempotent. Never called on a thread that must not block: see {@link BoundedCall#runAsync}. */
  @Override
  public void close() {
    if (closed.compareAndSet(false, true)) {
      transport.close();
    }
  }

  boolean isBroken() {
    return broken;
  }

  /**
   * Sends a request and returns its response messages. A failure with one of {@code
   * emptyReasons}, thrown or reported in a response message, yields no messages instead of an
   * error.
   */
  private List<PCFMessage> exchange(PCFMessage request, String objectName, int... emptyReasons) {
    if (broken || closed.get()) {
      throw new MqUnreachableException("connection closed after an earlier failure", null);
    }
    List<PCFMessage> responses;
    try {
      responses = boundedSend(request);
    } catch (Checked c) {
      if (c.getCause() instanceof MQException e) return failed(e.reasonCode, objectName, e, emptyReasons);
      if (c.getCause() instanceof MQDataException e) return failed(e.reasonCode, objectName, e, emptyReasons);
      throw new MqUnreachableException("PCF transport failed: " + c.getCause().getMessage(), c.getCause());
    } catch (MqUnreachableException e) {
      if (e.getCause() instanceof TimeoutException) {
        markBroken();
      }
      throw e;
    }
    List<PCFMessage> ok = new ArrayList<>();
    for (PCFMessage m : responses) {
      if (m.getCompCode() == CMQC.MQCC_OK) {
        ok.add(m);
      } else if (!contains(emptyReasons, m.getReason())) {
        throw PcfErrors.translate(m.getReason(), objectName, null);
      }
    }
    return ok;
  }

  /** Sends on a daemon thread, waiting at most for what is left of the per-sample deadline. */
  private List<PCFMessage> boundedSend(PCFMessage request) {
    Duration budget = timeout;
    if (deadlineSet) {
      long remaining = deadlineNanos - System.nanoTime();
      if (remaining <= 0) {
        markBroken();
        throw new MqUnreachableException(timeoutMessage(), new TimeoutException());
      }
      budget = Duration.ofNanos(remaining);
    }
    return BoundedCall.within(
            budget,
            timeoutMessage(),
            () -> transport.send(request),
            late -> {},
            cause -> {
              if (cause instanceof MQException
                  || cause instanceof MQDataException
                  || cause instanceof IOException) {
                return new Checked((Exception) cause);
              }
              if (cause instanceof RuntimeException r) return r;
              return new MqGatewayException("PCF exchange failed: " + cause, cause);
            });
  }

  private String timeoutMessage() {
    return "no PCF reply within " + BoundedCall.describe(timeout);
  }

  /** A timed-out exchange may still hold the connection; close it without blocking the caller. */
  private void markBroken() {
    broken = true;
    BoundedCall.runAsync("closing timed-out PCF connection", this::close);
  }

  /** Carries a checked transport failure out of the bounded call so it can be translated. */
  private static final class Checked extends RuntimeException {
    Checked(Exception cause) {
      super(cause);
    }
  }

  private static List<PCFMessage> failed(int reason, String objectName, Exception e, int[] empty) {
    if (contains(empty, reason)) return List.of();
    throw PcfErrors.translate(reason, objectName, e);
  }

  private static boolean contains(int[] values, int value) {
    for (int v : values) if (v == value) return true;
    return false;
  }

  private static String str(PCFMessage m, int parameter) {
    if (m.getParameter(parameter) == null) return null;
    try {
      String s = m.getStringParameterValue(parameter);
      if (s == null) return null;
      s = s.trim();
      return s.isEmpty() ? null : s;
    } catch (MQDataException e) {
      return null;
    }
  }

  private static Integer integer(PCFMessage m, int parameter) {
    if (m.getParameter(parameter) == null) return null;
    try {
      return m.getIntParameterValue(parameter);
    } catch (MQDataException e) {
      return null;
    }
  }

  private static int requiredInt(PCFMessage m, int parameter, String queue) {
    Integer v = integer(m, parameter);
    if (v == null) {
      throw new MqGatewayException("queue manager did not report attribute " + parameter + " for " + queue);
    }
    return v;
  }

  private static Integer age(PCFMessage s) {
    Integer v = integer(s, CMQCFC.MQIACF_OLDEST_MSG_AGE);
    return v == null || v < 0 ? null : v;
  }

  private static LocalDateTime dateTime(PCFMessage s, int dateParam, int timeParam) {
    String date = str(s, dateParam);
    String time = str(s, timeParam);
    if (date == null || time == null) return null;
    try {
      return LocalDateTime.of(LocalDate.parse(date, PUT_DATE), LocalTime.parse(time, PUT_TIME));
    } catch (DateTimeParseException e) {
      return null;
    }
  }

  private static String qmgrState(int code) {
    if (code == CMQCFC.MQQMSTA_RUNNING) return "RUNNING";
    if (code == CMQCFC.MQQMSTA_QUIESCING) return "QUIESCING";
    return "UNKNOWN";
  }

  /** MQPL_UNIX and MQPL_AIX share a value, so the reverse lookup is ambiguous; report UNIX. */
  private static String platformName(int platform) {
    if (platform == CMQC.MQPL_UNIX) return "UNIX";
    return lookupName(platform, "MQPL_.*", "MQPL_");
  }

  private static String lookupName(int value, String pattern, String prefix) {
    String name = MQConstants.lookup(value, pattern);
    if (name == null) return "UNKNOWN";
    return name.startsWith(prefix) ? name.substring(prefix.length()) : name;
  }
}
