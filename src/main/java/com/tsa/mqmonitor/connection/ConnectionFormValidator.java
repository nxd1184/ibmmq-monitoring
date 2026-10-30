package com.tsa.mqmonitor.connection;

import com.tsa.mqmonitor.config.MqMonitorProperties;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;
import org.springframework.validation.Errors;
import org.springframework.validation.Validator;

@Component
public class ConnectionFormValidator implements Validator {
  private final MqMonitorProperties.Tls defaults;

  @Autowired
  public ConnectionFormValidator(MqMonitorProperties props) {
    this(props.tls());
  }

  public ConnectionFormValidator(MqMonitorProperties.Tls defaults) {
    this.defaults = defaults;
  }

  @Override
  public boolean supports(Class<?> clazz) {
    return ConnectionForm.class.isAssignableFrom(clazz);
  }

  @Override
  public void validate(Object target, Errors errors) {
    var form = (ConnectionForm) target;
    TlsMode mode = form.getTlsMode();
    if (mode != TlsMode.TLS && mode != TlsMode.MUTUAL_TLS) {
      return;
    }
    if (!StringUtils.hasText(form.getCipherSpec())) {
      errors.rejectValue("cipherSpec", "tls.cipherSpec.required", "Cipher spec is required for TLS");
    }
    if (!StringUtils.hasText(form.getTruststorePath())
        && !StringUtils.hasText(defaults.truststorePath())) {
      errors.rejectValue(
          "truststorePath", "tls.truststore.required", "Truststore path is required for TLS");
    }
    if (mode == TlsMode.MUTUAL_TLS
        && !StringUtils.hasText(form.getKeystorePath())
        && !StringUtils.hasText(defaults.keystorePath())) {
      errors.rejectValue(
          "keystorePath", "tls.keystore.required", "Keystore path is required for mutual TLS");
    }
  }
}
