package com.tsa.mqmonitor;

import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;
import org.springframework.boot.builder.SpringApplicationBuilder;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class MissingEncryptionKeyTest {

    @Test
    void failsWithoutEncryptionKey() {
        var app = new SpringApplicationBuilder(MqMonitorApplication.class)
                .properties("mqmonitor.encryption-key=", "mqmonitor.admin.username=a", "mqmonitor.admin.password=b",
                        "spring.datasource.url=jdbc:postgresql://localhost:1/x", "spring.flyway.enabled=false");
        assertThatThrownBy(app::run).hasRootCauseInstanceOf(BindValidationException.class);
    }
}
