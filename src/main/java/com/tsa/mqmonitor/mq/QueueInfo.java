package com.tsa.mqmonitor.mq;

import java.time.LocalDateTime;

public record QueueInfo(
    String name,
    int depth,
    int maxDepth,
    Integer openInputCount,
    Integer openOutputCount,
    Integer oldestMessageAgeSeconds,
    LocalDateTime lastPut,
    LocalDateTime lastGet) {}
