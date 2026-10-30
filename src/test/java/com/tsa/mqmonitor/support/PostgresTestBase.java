package com.tsa.mqmonitor.support;

import org.springframework.boot.test.autoconfigure.actuate.observability.AutoConfigureObservability;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.postgresql.PostgreSQLContainer;

import java.util.Base64;

@SpringBootTest
@AutoConfigureMockMvc
@AutoConfigureObservability
@Testcontainers
public abstract class PostgresTestBase {

    // Started once per JVM and shared by every subclass; Ryuk removes it on exit.
    protected static final PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:16-alpine");

    static {
        postgres.start();
    }

    @DynamicPropertySource
    static void properties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("mqmonitor.admin.username", () -> "admin");
        registry.add("mqmonitor.admin.password", () -> "admin-pass");
        registry.add("mqmonitor.encryption-key", () -> Base64.getEncoder().encodeToString(new byte[32]));
    }
}
