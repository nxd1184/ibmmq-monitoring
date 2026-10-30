package com.tsa.mqmonitor.connection;

import java.time.Instant;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

@Table("mq_connection")
public record MqConnection(
    @Id Long id,
    String name,
    String host,
    int port,
    String queueManager,
    String channel,
    String username,
    String passwordEncrypted,
    ClientType clientType,
    TlsMode tlsMode,
    String cipherSpec,
    String truststorePath,
    String keystorePath,
    String certificateLabel,
    String queueFilter,
    boolean hideSystemQueues,
    int sampleIntervalSeconds,
    boolean prometheusExport,
    String replyQueueModel,
    Integer thresholdDepthMax,
    Integer thresholdDepthPercent,
    Integer thresholdNoConsumerSeconds,
    Integer thresholdOldestMessageAgeSeconds,
    Instant createdAt,
    Instant updatedAt) {}
