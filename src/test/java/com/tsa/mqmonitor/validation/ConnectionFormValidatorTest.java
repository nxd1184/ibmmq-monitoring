package com.tsa.mqmonitor.validation;

import static org.assertj.core.api.Assertions.assertThat;

import com.tsa.mqmonitor.config.MqMonitorProperties;
import com.tsa.mqmonitor.enumerable.TlsMode;
import com.tsa.mqmonitor.model.ConnectionForm;
import org.junit.jupiter.api.Test;
import org.springframework.validation.BeanPropertyBindingResult;
import org.springframework.validation.Errors;

class ConnectionFormValidatorTest {
  MqMonitorProperties.Tls noDefaults = new MqMonitorProperties.Tls(null, null, null, null);
  ConnectionFormValidator connectionFormValidator = new ConnectionFormValidator(noDefaults);

  ConnectionForm f(TlsMode m) {
    var f = new ConnectionForm();
    f.setTlsMode(m);
    return f;
  }

  Errors validate(ConnectionForm f) {
    var e = new BeanPropertyBindingResult(f, "form");
    connectionFormValidator.validate(f, e);
    return e;
  }

  @Test
  void blankQueueFilterIsTreatedAsWildcard() {
    var f = f(TlsMode.NONE);
    f.setQueueFilter(" ");
    assertThat(validate(f).hasErrors()).isFalse();
    assertThat(f.getQueueFilter()).isEqualTo("*");
  }

  @Test
  void noneNeedsNothing() {
    assertThat(validate(f(TlsMode.NONE)).hasErrors()).isFalse();
  }

  @Test
  void tlsNeedsCipherAndTruststore() {
    var e = validate(f(TlsMode.TLS));
    assertThat(e.getFieldError("cipherSpec").getCode()).isEqualTo("tls.cipherSpec.required");
    assertThat(e.getFieldError("truststorePath").getCode()).isEqualTo("tls.truststore.required");
  }

  @Test
  void mutualTlsNeedsKeystore() {
    var f = f(TlsMode.MUTUAL_TLS);
    f.setCipherSpec("ANY_TLS12_OR_HIGHER");
    f.setTruststorePath("/t.p12");
    assertThat(validate(f).getFieldError("keystorePath").getCode())
        .isEqualTo("tls.keystore.required");
  }

  @Test
  void configuredDefaultStoreSatisfiesRule() {
    var v2 = new ConnectionFormValidator(new MqMonitorProperties.Tls("/t.p12", "p", "/k.p12", "p"));
    var f = f(TlsMode.MUTUAL_TLS);
    f.setCipherSpec("ANY_TLS12_OR_HIGHER");
    var e = new BeanPropertyBindingResult(f, "form");
    v2.validate(f, e);
    assertThat(e.hasErrors()).isFalse();
  }
}
