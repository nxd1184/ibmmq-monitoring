package com.tsa.mqmonitor;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;

class MissingEncryptionKeyTest {

    /** Command-line arguments outrank application.yml, so a local default key cannot mask the check. */
    @Test
    void failsWithoutEncryptionKey() {
        var app = new SpringApplicationBuilder(MqMonitorApplication.class);
        assertThatThrownBy(() -> app.run(
                        "--mqmonitor.encryption-key=",
                        "--mqmonitor.admin.username=a",
                        "--mqmonitor.admin.password=b",
                        "--spring.datasource.url=jdbc:postgresql://localhost:1/x",
                        "--spring.flyway.enabled=false"))
                .hasRootCauseInstanceOf(BindValidationException.class);
    }
}
