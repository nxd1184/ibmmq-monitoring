package com.tsa.mqmonitor.service.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.ibm.mq.MQException;
import com.ibm.mq.constants.CMQC;
import com.ibm.mq.constants.CMQCFC;
import com.ibm.mq.constants.CMQXC;
import com.ibm.mq.headers.pcf.PCFException;
import com.ibm.mq.headers.pcf.PCFMessage;
import com.tsa.mqmonitor.exception.MqAuthorityException;
import com.tsa.mqmonitor.exception.MqGatewayException;
import com.tsa.mqmonitor.exception.MqUnreachableException;
import com.tsa.mqmonitor.model.ChannelInfo;
import com.tsa.mqmonitor.model.QueueInfo;
import com.tsa.mqmonitor.model.QueueManagerInfo;
import com.tsa.mqmonitor.service.PcfTransport;
import java.io.IOException;
import java.time.LocalDateTime;
import org.junit.jupiter.api.Test;

class PcfMqGatewayTest {
  FakeTransport fakeTransport = new FakeTransport();
  PcfMqGateway pcfMqGateway = new PcfMqGateway(fakeTransport);

  static PCFMessage resp(int command) {
    return new PCFMessage(CMQCFC.MQCFT_RESPONSE, command, 1, true);
  }

  static PCFMessage qResp(String name, int depth, int max) {
    var m = resp(CMQCFC.MQCMD_INQUIRE_Q);
    m.addParameter(CMQC.MQCA_Q_NAME, name);
    m.addParameter(CMQC.MQIA_CURRENT_Q_DEPTH, depth);
    m.addParameter(CMQC.MQIA_MAX_Q_DEPTH, max);
    return m;
  }

  static PCFMessage qStatusResp(String name, int in, int out, int age) {
    var m = resp(CMQCFC.MQCMD_INQUIRE_Q_STATUS);
    m.addParameter(CMQC.MQCA_Q_NAME, name);
    m.addParameter(CMQC.MQIA_OPEN_INPUT_COUNT, in);
    m.addParameter(CMQC.MQIA_OPEN_OUTPUT_COUNT, out);
    m.addParameter(CMQCFC.MQIACF_OLDEST_MSG_AGE, age);
    return m;
  }

  static PCFMessage qmgrStatusResp(String name, int status) {
    var m = resp(CMQCFC.MQCMD_INQUIRE_Q_MGR_STATUS);
    m.addParameter(CMQC.MQCA_Q_MGR_NAME, name);
    m.addParameter(CMQCFC.MQIACF_Q_MGR_STATUS, status);
    return m;
  }

  static PCFMessage qmgrResp(int commandLevel, int platform) {
    var m = resp(CMQCFC.MQCMD_INQUIRE_Q_MGR);
    m.addParameter(CMQC.MQIA_COMMAND_LEVEL, commandLevel);
    m.addParameter(CMQC.MQIA_PLATFORM, platform);
    return m;
  }

  static PCFMessage chlStatusResp(String name, int type, int status) {
    var m = resp(CMQCFC.MQCMD_INQUIRE_CHANNEL_STATUS);
    m.addParameter(CMQCFC.MQCACH_CHANNEL_NAME, name);
    m.addParameter(CMQCFC.MQIACH_CHANNEL_TYPE, type);
    m.addParameter(CMQCFC.MQIACH_CHANNEL_STATUS, status);
    return m;
  }

  @Test
  void inquireQueuesRequestsLocalQueuesWithFilterAndStatus() throws Exception {
    fakeTransport.respond(CMQCFC.MQCMD_INQUIRE_Q, qResp("APP.IN", 5, 5000), qResp("SYSTEM.ADMIN.X", 0, 100));
    fakeTransport.respond(CMQCFC.MQCMD_INQUIRE_Q_STATUS, qStatusResp("APP.IN", 2, 1, 42));
    var qs = pcfMqGateway.inquireQueues("APP.*", true);
    assertThat(fakeTransport.requests.get(0).getStringParameterValue(CMQC.MQCA_Q_NAME).trim()).isEqualTo("APP.*");
    assertThat(fakeTransport.requests.get(0).getIntParameterValue(CMQC.MQIA_Q_TYPE)).isEqualTo(CMQC.MQQT_LOCAL);
    assertThat(fakeTransport.requests.get(0).getIntListParameterValue(CMQCFC.MQIACF_Q_ATTRS))
        .containsExactly(CMQC.MQCA_Q_NAME, CMQC.MQIA_CURRENT_Q_DEPTH, CMQC.MQIA_MAX_Q_DEPTH);
    assertThat(fakeTransport.requests.get(1).getCommand()).isEqualTo(CMQCFC.MQCMD_INQUIRE_Q_STATUS);
    assertThat(fakeTransport.requests.get(1).getIntListParameterValue(CMQCFC.MQIACF_Q_STATUS_ATTRS)).hasSize(8);
    assertThat(qs).extracting(QueueInfo::name).containsExactly("APP.IN");
    assertThat(qs.get(0))
        .satisfies(
            q -> {
              assertThat(q.depth()).isEqualTo(5);
              assertThat(q.maxDepth()).isEqualTo(5000);
              assertThat(q.openInputCount()).isEqualTo(2);
              assertThat(q.openOutputCount()).isEqualTo(1);
              assertThat(q.oldestMessageAgeSeconds()).isEqualTo(42);
            });
  }

  @Test
  void systemQueuesShownWhenNotHidden() {
    fakeTransport.respond(CMQCFC.MQCMD_INQUIRE_Q, qResp("SYSTEM.ADMIN.X", 0, 100));
    fakeTransport.respond(CMQCFC.MQCMD_INQUIRE_Q_STATUS);
    assertThat(pcfMqGateway.inquireQueues("*", false)).hasSize(1);
  }

  @Test
  void amqDynamicQueuesAlwaysDropped() {
    fakeTransport.respond(CMQCFC.MQCMD_INQUIRE_Q, qResp("AMQ.5F1.ABC", 0, 100), qResp("APP.IN", 0, 100));
    assertThat(pcfMqGateway.inquireQueues("*", false)).extracting(QueueInfo::name).containsExactly("APP.IN");
  }

  @Test
  void monqOffYieldsNullAge() {
    fakeTransport.respond(CMQCFC.MQCMD_INQUIRE_Q, qResp("APP.IN", 0, 100));
    fakeTransport.respond(CMQCFC.MQCMD_INQUIRE_Q_STATUS, qStatusResp("APP.IN", 0, 0, -1));
    assertThat(pcfMqGateway.inquireQueues("*", true).get(0).oldestMessageAgeSeconds()).isNull();
  }

  @Test
  void missingStatusYieldsNullStatusFields() {
    fakeTransport.respond(CMQCFC.MQCMD_INQUIRE_Q, qResp("APP.IN", 3, 100));
    var q = pcfMqGateway.inquireQueues("*", true).get(0);
    assertThat(q.openInputCount()).isNull();
    assertThat(q.openOutputCount()).isNull();
    assertThat(q.oldestMessageAgeSeconds()).isNull();
    assertThat(q.lastPut()).isNull();
    assertThat(q.lastGet()).isNull();
  }

  @Test
  void parsesLastPutAndGet() {
    fakeTransport.respond(CMQCFC.MQCMD_INQUIRE_Q, qResp("APP.IN", 1, 100));
    var s = qStatusResp("APP.IN", 0, 0, 5);
    s.addParameter(CMQCFC.MQCACF_LAST_PUT_DATE, "2026-10-30");
    s.addParameter(CMQCFC.MQCACF_LAST_PUT_TIME, "13.45.07");
    s.addParameter(CMQCFC.MQCACF_LAST_GET_DATE, "2026-10-29");
    s.addParameter(CMQCFC.MQCACF_LAST_GET_TIME, "01.02.03");
    fakeTransport.respond(CMQCFC.MQCMD_INQUIRE_Q_STATUS, s);
    var q = pcfMqGateway.inquireQueues("*", true).get(0);
    assertThat(q.lastPut()).isEqualTo(LocalDateTime.of(2026, 10, 30, 13, 45, 7));
    assertThat(q.lastGet()).isEqualTo(LocalDateTime.of(2026, 10, 29, 1, 2, 3));
  }

  @Test
  void blankLastPutIsNull() {
    fakeTransport.respond(CMQCFC.MQCMD_INQUIRE_Q, qResp("APP.IN", 1, 100));
    var s = qStatusResp("APP.IN", 0, 0, 5);
    s.addParameter(CMQCFC.MQCACF_LAST_PUT_DATE, "        ");
    s.addParameter(CMQCFC.MQCACF_LAST_PUT_TIME, "        ");
    fakeTransport.respond(CMQCFC.MQCMD_INQUIRE_Q_STATUS, s);
    assertThat(pcfMqGateway.inquireQueues("*", true).get(0).lastPut()).isNull();
  }

  @Test
  void unknownQueueStatusNameIsNoStatus() throws Exception {
    fakeTransport.respond(CMQCFC.MQCMD_INQUIRE_Q, qResp("APP.IN", 1, 100));
    fakeTransport.fail(CMQCFC.MQCMD_INQUIRE_Q_STATUS, new PCFException(2, CMQC.MQRC_UNKNOWN_OBJECT_NAME, null));
    assertThat(pcfMqGateway.inquireQueues("*", true)).hasSize(1);
  }

  @Test
  void statusErrorResponseMessageIsTranslated() {
    fakeTransport.respond(CMQCFC.MQCMD_INQUIRE_Q, qResp("APP.IN", 1, 100));
    var bad = resp(CMQCFC.MQCMD_INQUIRE_Q_STATUS);
    bad.getHeader().setCompCode(2);
    bad.getHeader().setReason(CMQC.MQRC_NOT_AUTHORIZED);
    fakeTransport.respond(CMQCFC.MQCMD_INQUIRE_Q_STATUS, bad);
    assertThatThrownBy(() -> pcfMqGateway.inquireQueues("*", true)).isInstanceOf(MqAuthorityException.class);
  }

  @Test
  void messageWithoutNameIsSkipped() {
    fakeTransport.respond(CMQCFC.MQCMD_INQUIRE_Q, qResp("APP.IN", 1, 100), resp(CMQCFC.MQCMD_INQUIRE_Q));
    assertThat(pcfMqGateway.inquireQueues("*", true)).hasSize(1);
  }

  @Test
  void channelsMapTypeAndStatus() throws Exception {
    fakeTransport.respond(CMQCFC.MQCMD_INQUIRE_CHANNEL_STATUS, chlStatusResp("TO.QM2", CMQXC.MQCHT_SENDER, CMQCFC.MQCHS_RUNNING));
    assertThat(pcfMqGateway.inquireChannels()).containsExactly(new ChannelInfo("TO.QM2", "SENDER", "RUNNING"));
    var req = fakeTransport.requests.get(0);
    assertThat(req.getStringParameterValue(CMQCFC.MQCACH_CHANNEL_NAME).trim()).isEqualTo("*");
    assertThat(req.getIntParameterValue(CMQCFC.MQIACH_CHANNEL_INSTANCE_TYPE)).isEqualTo(CMQC.MQOT_CURRENT_CHANNEL);
  }

  @Test
  void noChannelStatusIsEmptyList() {
    fakeTransport.fail(CMQCFC.MQCMD_INQUIRE_CHANNEL_STATUS, new PCFException(2, CMQCFC.MQRCCF_CHL_STATUS_NOT_FOUND, null));
    assertThat(pcfMqGateway.inquireChannels()).isEmpty();
  }

  @Test
  void noChannelStatusAsResponseMessageIsEmptyList() {
    var m = resp(CMQCFC.MQCMD_INQUIRE_CHANNEL_STATUS);
    m.getHeader().setCompCode(2);
    m.getHeader().setReason(CMQCFC.MQRCCF_CHL_STATUS_NOT_FOUND);
    fakeTransport.respond(CMQCFC.MQCMD_INQUIRE_CHANNEL_STATUS, m);
    assertThat(pcfMqGateway.inquireChannels()).isEmpty();
  }

  @Test
  void notAuthorizedBecomesAuthorityException() {
    fakeTransport.fail(CMQCFC.MQCMD_INQUIRE_Q, new PCFException(2, CMQC.MQRC_NOT_AUTHORIZED, null));
    assertThatThrownBy(() -> pcfMqGateway.inquireQueues("*", true))
        .isInstanceOf(MqAuthorityException.class)
        .hasMessageContaining("2035");
  }

  @Test
  void plainMqExceptionIsTranslatedToo() {
    fakeTransport.fail(CMQCFC.MQCMD_INQUIRE_Q, new MQException(2, CMQC.MQRC_Q_MGR_NOT_AVAILABLE, null));
    assertThatThrownBy(() -> pcfMqGateway.inquireQueues("*", true)).isInstanceOf(MqUnreachableException.class);
  }

  @Test
  void ioExceptionBecomesUnreachable() {
    fakeTransport.fail(CMQCFC.MQCMD_INQUIRE_Q, new IOException("boom"));
    assertThatThrownBy(() -> pcfMqGateway.inquireQueues("*", true)).isInstanceOf(MqUnreachableException.class);
  }

  @Test
  void filterMatchingNoQueueIsEmptyList() {
    fakeTransport.fail(CMQCFC.MQCMD_INQUIRE_Q, new PCFException(2, CMQC.MQRC_UNKNOWN_OBJECT_NAME, null));
    fakeTransport.fail(CMQCFC.MQCMD_INQUIRE_Q_STATUS, new PCFException(2, CMQC.MQRC_UNKNOWN_OBJECT_NAME, null));
    assertThat(pcfMqGateway.inquireQueues("NOMATCH.*", true)).isEmpty();
  }

  @Test
  void otherReasonBecomesGatewayException() {
    fakeTransport.fail(CMQCFC.MQCMD_INQUIRE_Q, new PCFException(2, CMQC.MQRC_NO_MSG_AVAILABLE, null));
    assertThatThrownBy(() -> pcfMqGateway.inquireQueues("*", true))
        .isExactlyInstanceOf(MqGatewayException.class);
  }

  @Test
  void qmgrStatusMapsToName() {
    fakeTransport.respond(CMQCFC.MQCMD_INQUIRE_Q_MGR_STATUS, qmgrStatusResp("QM1", CMQCFC.MQQMSTA_RUNNING));
    fakeTransport.respond(CMQCFC.MQCMD_INQUIRE_Q_MGR, qmgrResp(940, CMQC.MQPL_UNIX));
    var info = pcfMqGateway.inquireQueueManager();
    assertThat(info).isEqualTo(new QueueManagerInfo("QM1", "RUNNING", 940, "UNIX"));
    assertThat(fakeTransport.requests).extracting(PCFMessage::getCommand)
        .containsExactly(CMQCFC.MQCMD_INQUIRE_Q_MGR_STATUS, CMQCFC.MQCMD_INQUIRE_Q_MGR);
  }

  @Test
  void closeClosesTransport() {
    pcfMqGateway.close();
    assertThat(fakeTransport.closed).isTrue();
  }

  /** Answers every request with an empty OK reply after {@code delayMs}. */
  static final class SlowTransport implements PcfTransport {
    final long delayMs;
    volatile boolean closed;

    SlowTransport(long delayMs) {
      this.delayMs = delayMs;
    }

    @Override
    public java.util.List<PCFMessage> send(PCFMessage request) {
      try {
        Thread.sleep(delayMs);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
      return java.util.List.of();
    }

    @Override
    public void close() {
      closed = true;
    }
  }

  @Test
  void oneDeadlineIsSharedByEveryExchangeOfASample() throws Exception {
    var slow = new SlowTransport(150);
    var bounded = new PcfMqGateway(slow, java.time.Duration.ofMillis(400));
    bounded.beginSample();
    bounded.inquireQueueManager(); // two exchanges, ~300ms
    long start = System.nanoTime();
    assertThatThrownBy(() -> bounded.inquireQueues("*", true)) // would need ~300ms more
        .isInstanceOf(MqUnreachableException.class)
        .hasMessage("no PCF reply within 400ms");
    assertThat(java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start))
        .isLessThan(1_000);
    assertThat(bounded.isBroken()).isTrue();
    long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(2);
    while (!slow.closed && System.nanoTime() < deadline) Thread.sleep(10);
    assertThat(slow.closed).as("closed asynchronously").isTrue();
    assertThatThrownBy(bounded::inquireChannels).isInstanceOf(MqUnreachableException.class);
  }

  @Test
  void newSampleGetsAFreshBudget() {
    var slow = new SlowTransport(150);
    var bounded = new PcfMqGateway(slow, java.time.Duration.ofMillis(400));
    bounded.beginSample();
    bounded.inquireQueueManager();
    bounded.beginSample();
    bounded.inquireQueueManager();
    assertThat(bounded.isBroken()).isFalse();
  }

  @Test
  void connectTimeAuthorityFailureNamesCredentials() {
    assertThat(PcfErrors.translateConnect(CMQC.MQRC_NOT_AUTHORIZED, "QM1", null))
        .isInstanceOf(MqAuthorityException.class)
        .hasMessage("not authorized: check username/password or +connect authority (MQRC 2035)");
    assertThat(PcfErrors.translate(CMQC.MQRC_NOT_AUTHORIZED, "DEV.*", null))
        .hasMessage("authority missing on DEV.* (MQRC 2035)");
  }
}
