package com.tsa.mqmonitor.config;

import com.tsa.mqmonitor.util.Redact;
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

    /** Never prints the encryption key. */
    @Override
    public String toString() {
        return "MqMonitorProperties[admin=" + admin + ", encryptionKey=" + Redact.secret(encryptionKey)
                + ", tls=" + tls + ", sampling=" + sampling + ", thresholds=" + thresholds + ", mq=" + mq + "]";
    }

    public record Admin(@NotBlank String username, @NotBlank String password) {
        @Override
        public String toString() {
            return "Admin[username=" + username + ", password=" + Redact.secret(password) + "]";
        }
    }

    public record Tls(String truststorePath, String truststorePassword, String keystorePath, String keystorePassword) {
        @Override
        public String toString() {
            return "Tls[truststorePath=" + truststorePath + ", truststorePassword=" + Redact.secret(truststorePassword)
                    + ", keystorePath=" + keystorePath + ", keystorePassword=" + Redact.secret(keystorePassword) + "]";
        }
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
