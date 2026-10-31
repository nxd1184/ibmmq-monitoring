package com.tsa.mqmonitor.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.tsa.mqmonitor.exception.MqGatewayException;
import com.tsa.mqmonitor.model.TlsMode;
import com.tsa.mqmonitor.model.TlsSettings;
import java.nio.file.Path;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SslContextFactoryTest {
  @TempDir static Path dir;
  static String p12;

  @BeforeAll
  static void generateKeystore() throws Exception {
    Path store = dir.resolve("client.p12");
    Path keytool = Path.of(System.getProperty("java.home"), "bin", "keytool");
    Process p =
        new ProcessBuilder(
                keytool.toString(), "-genkeypair", "-storetype", "PKCS12",
                "-keystore", store.toString(), "-storepass", "changeit",
                "-alias", "client", "-keyalg", "RSA", "-keysize", "2048",
                "-dname", "CN=mq-monitor-test", "-validity", "2")
            .redirectErrorStream(true)
            .start();
    String out = new String(p.getInputStream().readAllBytes());
    assertThat(p.waitFor()).as(out).isZero();
    p12 = store.toString();
  }

  @Test
  void noneReturnsNull() {
    assertThat(SslContextFactory.build(TlsSettings.none())).isNull();
  }

  @Test
  void tlsBuildsFactoryFromTruststore() {
    assertThat(
            SslContextFactory.build(
                new TlsSettings(TlsMode.TLS, "ANY_TLS12_OR_HIGHER", p12, "changeit", null, null, null)))
        .isNotNull();
  }

  @Test
  void mutualTlsWithWrongPasswordFails() {
    assertThatThrownBy(
            () ->
                SslContextFactory.build(
                    new TlsSettings(
                        TlsMode.MUTUAL_TLS, "ANY_TLS12_OR_HIGHER", p12, "changeit", p12, "wrong", null)))
        .isInstanceOf(MqGatewayException.class)
        .hasMessageContaining("keystore");
  }
}
