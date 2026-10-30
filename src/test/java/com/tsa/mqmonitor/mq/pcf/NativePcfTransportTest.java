package com.tsa.mqmonitor.mq.pcf;

import static org.assertj.core.api.Assertions.assertThat;

import com.ibm.mq.constants.MQConstants;
import com.tsa.mqmonitor.connection.ClientType;
import com.tsa.mqmonitor.connection.TlsMode;
import com.tsa.mqmonitor.mq.MqTarget;
import com.tsa.mqmonitor.mq.TlsSettings;
import java.time.Duration;
import javax.net.ssl.SSLSocketFactory;
import org.junit.jupiter.api.Test;

class NativePcfTransportTest {
  static MqTarget target(String user, TlsSettings tls) {
    return new MqTarget("h", 1414, "QM1", "CH", user, "pw", ClientType.NATIVE, tls,
        "SYSTEM.DEFAULT.MODEL.QUEUE", Duration.ofSeconds(5));
  }

  @Test
  void credentialsOnlyWhenUsernameGiven() {
    var props = NativePcfTransport.properties(target(null, TlsSettings.none()), null);
    assertThat(props).doesNotContainKeys(
        MQConstants.USER_ID_PROPERTY, MQConstants.PASSWORD_PROPERTY,
        MQConstants.USE_MQCSP_AUTHENTICATION_PROPERTY, MQConstants.SSL_SOCKET_FACTORY_PROPERTY);
    assertThat(props).containsEntry(MQConstants.APPNAME_PROPERTY, "mq-monitor");

    var withUser = NativePcfTransport.properties(target("admin", TlsSettings.none()), null);
    assertThat(withUser)
        .containsEntry(MQConstants.USER_ID_PROPERTY, "admin")
        .containsEntry(MQConstants.USE_MQCSP_AUTHENTICATION_PROPERTY, true);
  }

  @Test
  void tlsSetsSocketFactoryAndCipherSuite() {
    var ssl = (SSLSocketFactory) SSLSocketFactory.getDefault();
    var tls = new TlsSettings(TlsMode.TLS, "ANY_TLS12_OR_HIGHER", null, null, null, null, null);
    assertThat(NativePcfTransport.properties(target("admin", tls), ssl))
        .containsEntry(MQConstants.SSL_SOCKET_FACTORY_PROPERTY, ssl)
        .containsEntry(MQConstants.SSL_CIPHER_SUITE_PROPERTY, "*TLS12ORHIGHER");
  }
}
