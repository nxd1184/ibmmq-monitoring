package com.tsa.mqmonitor.sampling;

import com.tsa.mqmonitor.health.HealthVerdict;
import java.util.Map;

public record EvaluatedSample(
    Sample sample,
    HealthVerdict connection,
    Map<String, HealthVerdict> queues,
    Map<String, HealthVerdict> channels) {}
