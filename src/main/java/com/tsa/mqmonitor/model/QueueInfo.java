package com.tsa.mqmonitor.model;

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
