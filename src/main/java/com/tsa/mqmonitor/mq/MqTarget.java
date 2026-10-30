package com.tsa.mqmonitor.mq;

import com.tsa.mqmonitor.connection.ClientType;
import java.time.Duration;

public record MqTarget(
    String host,
    int port,
    String queueManager,
    String channel,
    String username,
    String password,
    ClientType clientType,
    TlsSettings tls,
    String replyQueueModel,
    Duration connectTimeout) {}
