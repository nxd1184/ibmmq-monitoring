package com.tsa.mqmonitor.model;

import java.util.Map;

public record EvaluatedSample(
    Sample sample,
    HealthVerdict connection,
    Map<String, HealthVerdict> queues,
    Map<String, HealthVerdict> channels) {}
