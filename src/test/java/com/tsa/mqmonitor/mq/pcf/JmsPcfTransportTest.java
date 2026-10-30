package com.tsa.mqmonitor.mq.pcf;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.ibm.mq.MQException;
import com.ibm.mq.constants.CMQC;
import com.ibm.mq.constants.CMQCFC;
import com.ibm.mq.headers.pcf.PCFException;
import com.ibm.mq.headers.pcf.PCFMessage;
import com.ibm.mq.jakarta.jms.MQConnectionFactory;
import com.ibm.msg.client.jakarta.wmq.WMQConstants;
import com.tsa.mqmonitor.connection.ClientType;
import com.tsa.mqmonitor.connection.TlsMode;
import com.tsa.mqmonitor.mq.MqAuthorityException;
import com.tsa.mqmonitor.mq.MqGatewayException;
import com.tsa.mqmonitor.mq.MqTarget;
import com.tsa.mqmonitor.mq.MqUnreachableException;
import com.tsa.mqmonitor.mq.TlsSettings;
import jakarta.jms.JMSException;
import java.time.Duration;
import java.util.List;
import javax.net.ssl.SSLSocketFactory;
import org.junit.jupiter.api.Test;

class JmsPcfTransportTest {
  static MqTarget target(String user, TlsSettings tls, String model) {
    return new MqTarget("h", 1414, "QM1", "CH", user, "pw", ClientType.SPRING_JMS, tls,
        model, Duration.ofSeconds(5));
  }

  @Test
  void clientConnectionWithoutCredentialsOrTls() throws Exception {
    MQConnectionFactory cf =
        JmsPcfTransport.connectionFactory(target(null, TlsSettings.none(), "SYSTEM.DEFAULT.MODEL.QUEUE"), null);
    assertThat(cf.getTransportType()).isEqualTo(WMQConstants.WMQ_CM_CLIENT);
    assertThat(cf.getHostName()).isEqualTo("h");
    assertThat(cf.getPort()).isEqualTo(1414);
    assertThat(cf.getChannel()).isEqualTo("CH");
    assertThat(cf.getQueueManager()).isEqualTo("QM1");
    assertThat(cf.getAppName()).isEqualTo("mq-monitor");
    assertThat(cf.getClientReconnectOptions()).isEqualTo(WMQConstants.WMQ_CLIENT_RECONNECT_DISABLED);
    assertThat(cf.getStringProperty(WMQConstants.USERID)).isNullOrEmpty();
    assertThat(cf.getSSLSocketFactory()).isNull();
    assertThat(cf.getSSLCipherSuite()).isNull();
    assertThat(cf.getTemporaryModel()).isEqualTo("SYSTEM.DEFAULT.MODEL.QUEUE");
  }

  @Test
  void credentialsUseMqcspWhenUsernameGiven() throws Exception {
    MQConnectionFactory cf =
        JmsPcfTransport.connectionFactory(target("admin", TlsSettings.none(), "SYSTEM.DEFAULT.MODEL.QUEUE"), null);
    assertThat(cf.getStringProperty(WMQConstants.USERID)).isEqualTo("admin");
    assertThat(cf.getStringProperty(WMQConstants.PASSWORD)).isEqualTo("pw");
    assertThat(cf.getBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP)).isTrue();
  }

  @Test
  void tlsSetsSocketFactoryAndMappedCipherSuite() throws Exception {
    var ssl = (SSLSocketFactory) SSLSocketFactory.getDefault();
    var tls = new TlsSettings(TlsMode.TLS, "ANY_TLS12_OR_HIGHER", null, null, null, null, null);
    MQConnectionFactory cf = JmsPcfTransport.connectionFactory(target("admin", tls, "SYSTEM.DEFAULT.MODEL.QUEUE"), ssl);
    assertThat(cf.getSSLSocketFactory()).isSameAs(ssl);
    assertThat(cf.getSSLCipherSuite()).isEqualTo("*TLS12ORHIGHER");
  }

  @Test
  void customReplyModelBecomesTemporaryModel() throws Exception {
    MQConnectionFactory cf = JmsPcfTransport.connectionFactory(target(null, TlsSettings.none(), "MY.MODEL"), null);
    assertThat(cf.getTemporaryModel()).isEqualTo("MY.MODEL");
  }

  @Test
  void linkedMqReasonIsTranslated() {
    var e = new JMSException("JMSWMQ2013: auth", "JMSWMQ2013");
    e.setLinkedException(new MQException(CMQC.MQCC_FAILED, CMQC.MQRC_NOT_AUTHORIZED, null));
    assertThat(JmsPcfTransport.translate(e, "QM1")).isInstanceOf(MqAuthorityException.class);

    var broken = new JMSException("JMSWMQ0018: connect", "JMSWMQ0018");
    broken.setLinkedException(new MQException(CMQC.MQCC_FAILED, CMQC.MQRC_HOST_NOT_AVAILABLE, null));
    assertThat(JmsPcfTransport.translate(broken, "QM1"))
        .isInstanceOf(MqUnreachableException.class)
        .hasMessageContaining("2538");
  }

  @Test
  void errorCodeIsUsedWhenNoMqReasonIsLinked() {
    assertThat(JmsPcfTransport.translate(new JMSException("auth", "JMSWMQ2013"), "QM1"))
        .isInstanceOf(MqAuthorityException.class);
    assertThat(JmsPcfTransport.translate(new JMSException("connect", "JMSWMQ0018"), "QM1"))
        .isInstanceOf(MqUnreachableException.class);
    assertThat(JmsPcfTransport.translate(new JMSException("other", "JMSCC0001"), "QM1"))
        .isExactlyInstanceOf(MqGatewayException.class);
  }

  static PCFMessage response(int seq, boolean last, int compCode, int reason) {
    var m = new PCFMessage(CMQCFC.MQCFT_RESPONSE, CMQCFC.MQCMD_INQUIRE_Q, seq, last);
    m.getHeader().setReason(reason);
    m.getHeader().setCompCode(compCode);
    return m;
  }

  @Test
  void commandFailedSummaryReportsTheFirstRealReason() {
    var responses = List.of(
        response(1, false, CMQC.MQCC_FAILED, CMQC.MQRC_UNKNOWN_OBJECT_NAME),
        response(2, true, CMQC.MQCC_FAILED, CMQCFC.MQRCCF_COMMAND_FAILED));
    assertThatThrownBy(() -> JmsPcfTransport.check(responses))
        .isInstanceOfSatisfying(PCFException.class,
            e -> assertThat(e.reasonCode).isEqualTo(CMQC.MQRC_UNKNOWN_OBJECT_NAME));
  }

  @Test
  void singleFailedResponseReportsItsReason() {
    var responses = List.of(response(1, true, CMQC.MQCC_FAILED, CMQC.MQRC_NOT_AUTHORIZED));
    assertThatThrownBy(() -> JmsPcfTransport.check(responses))
        .isInstanceOfSatisfying(PCFException.class,
            e -> assertThat(e.reasonCode).isEqualTo(CMQC.MQRC_NOT_AUTHORIZED));
  }

  @Test
  void successfulResponsesAreReturned() throws Exception {
    var responses = List.of(
        response(1, false, CMQC.MQCC_OK, CMQC.MQRC_NONE),
        response(2, true, CMQC.MQCC_OK, CMQC.MQRC_NONE));
    assertThat(JmsPcfTransport.check(responses)).isSameAs(responses);
  }
}
