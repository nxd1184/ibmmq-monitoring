package com.tsa.mqmonitor.model;

import static org.assertj.core.api.Assertions.assertThat;

import com.tsa.mqmonitor.config.MqMonitorProperties;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class SecretToStringTest {
  static final String SECRET = "s3cret-Pa55";

  @Test
  void targetAndTlsSettingsRedactPasswords() {
    var tls = new TlsSettings(TlsMode.MUTUAL_TLS, "ANY_TLS12", "/t.p12", SECRET, "/k.p12", SECRET, "lbl");
    var target =
        new MqTarget("mq", 1414, "QM1", "DEV.APP.SVRCONN", "app", SECRET, ClientType.NATIVE, tls,
            "SYSTEM.DEFAULT.MODEL.QUEUE", Duration.ofSeconds(10));
    assertThat(target.toString()).doesNotContain(SECRET).contains("password=***").contains("host=mq");
    assertThat(tls.toString()).doesNotContain(SECRET).contains("truststorePassword=***");
  }

  @Test
  void propertiesRedactAdminPasswordStorePasswordsAndKey() {
    var props =
        new MqMonitorProperties(
            new MqMonitorProperties.Admin("admin", SECRET),
            SECRET,
            new MqMonitorProperties.Tls("/t.p12", SECRET, "/k.p12", SECRET),
            new MqMonitorProperties.Sampling(30, 30),
            new MqMonitorProperties.Thresholds(null, 80, 60, null),
            new MqMonitorProperties.Mq(10));
    assertThat(props.toString()).doesNotContain(SECRET).contains("username=admin");
    assertThat(props.admin().toString()).doesNotContain(SECRET);
    assertThat(props.tls().toString()).doesNotContain(SECRET);
  }
}
