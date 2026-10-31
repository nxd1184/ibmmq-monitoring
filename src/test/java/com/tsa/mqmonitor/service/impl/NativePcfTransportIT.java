package com.tsa.mqmonitor.service.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.tsa.mqmonitor.config.MqMonitorProperties;
import com.tsa.mqmonitor.enumerable.ClientType;
import com.tsa.mqmonitor.enumerable.TlsMode;
import com.tsa.mqmonitor.exception.MqGatewayException;
import com.tsa.mqmonitor.exception.MqUnreachableException;
import com.tsa.mqmonitor.model.ChannelInfo;
import com.tsa.mqmonitor.model.MqTarget;
import com.tsa.mqmonitor.model.QueueInfo;
import com.tsa.mqmonitor.model.TlsSettings;
import com.tsa.mqmonitor.service.MqGateway;
import com.tsa.mqmonitor.support.MqContainer;
import java.time.Duration;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
class NativePcfTransportIT {
  static final String MODEL = "SYSTEM.DEFAULT.MODEL.QUEUE";

  final DefaultMqGatewayFactory defaultMqGatewayFactory = new DefaultMqGatewayFactory(props());

  static MqContainer mq() {
    return MqContainer.instance();
  }

  static MqMonitorProperties props() {
    return new MqMonitorProperties(
        new MqMonitorProperties.Admin("admin", "admin-pass"),
        "key",
        new MqMonitorProperties.Tls(null, null, null, null),
        new MqMonitorProperties.Sampling(30, 30),
        new MqMonitorProperties.Thresholds(null, 80, 60, null),
        new MqMonitorProperties.Mq(10));
  }

  static MqTarget target(MqContainer c, String channel, String user, String pw, TlsSettings tls, String model) {
    return new MqTarget(
        c.getHost(), c.getMappedPort(1414), "QM1", channel, user, pw,
        ClientType.NATIVE, tls, model, Duration.ofSeconds(10));
  }

  MqGateway open(String user, String pw) {
    return defaultMqGatewayFactory.open(target(mq(), "DEV.ADMIN.SVRCONN", user, pw, TlsSettings.none(), MODEL));
  }

  MqGateway open() {
    return open("admin", MqContainer.PASSWORD);
  }

  @Test
  void readsQueueManagerAndDevQueues() {
    try (var gw = open()) {
      var qm = gw.inquireQueueManager();
      assertThat(qm.name()).isEqualTo("QM1");
      assertThat(qm.status()).isEqualTo("RUNNING");
      assertThat(qm.commandLevel()).isPositive();
      assertThat(gw.inquireQueues("DEV.*", true))
          .extracting(QueueInfo::name)
          .contains("DEV.QUEUE.1", "DEV.QUEUE.2", "DEV.QUEUE.3");
      assertThat(gw.inquireChannels()).extracting(ChannelInfo::name).contains("DEV.ADMIN.SVRCONN");
    }
  }

  @Test
  void filterMatchingNothingIsEmptyList() {
    try (var gw = open()) {
      assertThat(gw.inquireQueues("NOMATCH.*", true)).isEmpty();
    }
  }

  @Test
  void depthReflectsMessagesPutByAnotherClient() {
    mq().mqsc("DEFINE QLOCAL(MON.DEPTH) REPLACE", "CLEAR QLOCAL(MON.DEPTH)");
    mq().exec("/opt/mqm/samp/bin/amqsput MON.DEPTH QM1", "one\ntwo\nthree\n");
    try (var gw = open()) {
      assertThat(gw.inquireQueues("MON.DEPTH", true).get(0).depth()).isEqualTo(3);
    }
  }

  @Test
  void monqOffGivesNullAgeAndMonqOnGivesAge() {
    mq().mqsc("DEFINE QLOCAL(MON.AGE) MONQ(OFF) REPLACE", "CLEAR QLOCAL(MON.AGE)");
    mq().exec("/opt/mqm/samp/bin/amqsput MON.AGE QM1", "aged\n");
    try (var gw = open()) {
      assertThat(gw.inquireQueues("MON.AGE", true).get(0).oldestMessageAgeSeconds()).isNull();
      mq().mqsc("ALTER QLOCAL(MON.AGE) MONQ(MEDIUM)");
      assertThat(gw.inquireQueues("MON.AGE", true).get(0).oldestMessageAgeSeconds())
          .isNotNull()
          .isGreaterThanOrEqualTo(0);
    }
  }

  @Test
  void customReplyModelIsUsed() {
    var t = target(mq(), "DEV.ADMIN.SVRCONN", "admin", MqContainer.PASSWORD, TlsSettings.none(), "NO.SUCH.MODEL");
    assertThatThrownBy(() -> defaultMqGatewayFactory.open(t))
        .isInstanceOf(MqGatewayException.class)
        .hasMessageContaining("2085");
  }

  @Test
  void wrongPasswordIsUnreachableOrAuthority() {
    assertThatThrownBy(() -> open("admin", "bad"))
        .isInstanceOf(MqGatewayException.class)
        .hasMessageContaining("2035");
  }

  @Test
  void blackholeHostTimesOut() {
    var t = new MqTarget(
        "10.255.255.1", 1414, "QM1", "X", null, null, ClientType.NATIVE,
        TlsSettings.none(), MODEL, Duration.ofSeconds(2));
    long start = System.nanoTime();
    assertThatThrownBy(() -> defaultMqGatewayFactory.open(t)).isInstanceOf(MqUnreachableException.class);
    assertThat(Duration.ofNanos(System.nanoTime() - start)).isLessThan(Duration.ofSeconds(5));
  }

  @Test
  void tlsChannelConnects() {
    var c = MqContainer.tlsInstance();
    var tls = new TlsSettings(
        TlsMode.TLS, "ANY_TLS12_OR_HIGHER", c.truststore(), MqContainer.STORE_PASSWORD, null, null, null);
    try (var gw = defaultMqGatewayFactory.open(target(c, "DEV.ADMIN.SVRCONN", "admin", MqContainer.PASSWORD, tls, MODEL))) {
      assertThat(gw.inquireQueueManager().status()).isEqualTo("RUNNING");
    }
  }

  @Test
  void mutualTlsChannelConnects() {
    var c = MqContainer.tlsInstance();
    var tls = new TlsSettings(
        TlsMode.MUTUAL_TLS, "ANY_TLS12_OR_HIGHER",
        c.truststore(), MqContainer.STORE_PASSWORD,
        c.keystore(), MqContainer.STORE_PASSWORD, MqContainer.CLIENT_LABEL);
    try (var gw = defaultMqGatewayFactory.open(target(c, "DEV.MTLS.SVRCONN", "admin", MqContainer.PASSWORD, tls, MODEL))) {
      assertThat(gw.inquireQueueManager().status()).isEqualTo("RUNNING");
    }
  }

  @Test
  void mutualTlsChannelRejectsClientWithoutCertificate() {
    var c = MqContainer.tlsInstance();
    var tls = new TlsSettings(
        TlsMode.TLS, "ANY_TLS12_OR_HIGHER", c.truststore(), MqContainer.STORE_PASSWORD, null, null, null);
    var t = target(c, "DEV.MTLS.SVRCONN", "admin", MqContainer.PASSWORD, tls, MODEL);
    assertThatThrownBy(() -> defaultMqGatewayFactory.open(t)).isInstanceOf(MqGatewayException.class);
  }
}
