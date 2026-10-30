package com.tsa.mqmonitor.config;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "mqmonitor")
public record MqMonitorProperties(
        @Valid @DefaultValue Admin admin,
        @NotBlank String encryptionKey,
        @DefaultValue Tls tls,
        @DefaultValue Sampling sampling,
        @DefaultValue Thresholds thresholds,
        @DefaultValue Mq mq) {

    public record Admin(@NotBlank String username, @NotBlank String password) {
    }

    public record Tls(String truststorePath, String truststorePassword, String keystorePath, String keystorePassword) {
    }

    public record Sampling(@DefaultValue("30") int watcherGraceSeconds, @DefaultValue("30") int listIntervalSeconds) {
    }

    public record Thresholds(Integer depthMax,
                             @DefaultValue("80") int depthPercent,
                             @DefaultValue("60") int noConsumerSeconds,
                             Integer oldestMessageAgeSeconds) {
    }

    public record Mq(@DefaultValue("10") int connectTimeoutSeconds) {
    }
}
