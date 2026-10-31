package com.tsa.mqmonitor;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.tsa.mqmonitor.support.PostgresTestBase;
import org.junit.jupiter.api.Test;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.context.properties.bind.validation.BindValidationException;

class MissingEncryptionKeyTest {

    /**
     * Command-line arguments outrank application.yml, so a local default key cannot mask the check.
     * The database is reachable so that the only startup failure is the missing key: Spring creates
     * beans in scan order, and an unreachable data source would otherwise fail first.
     */
    @Test
    void failsWithoutEncryptionKey() {
        var postgres = PostgresTestBase.postgres();
        var app = new SpringApplicationBuilder(MqMonitorApplication.class);
        assertThatThrownBy(() -> app.run(
                        "--mqmonitor.encryption-key=",
                        "--mqmonitor.admin.username=a",
                        "--mqmonitor.admin.password=b",
                        "--spring.datasource.url=" + postgres.getJdbcUrl(),
                        "--spring.datasource.username=" + postgres.getUsername(),
                        "--spring.datasource.password=" + postgres.getPassword(),
                        "--spring.flyway.enabled=false"))
                .hasRootCauseInstanceOf(BindValidationException.class);
    }
}
