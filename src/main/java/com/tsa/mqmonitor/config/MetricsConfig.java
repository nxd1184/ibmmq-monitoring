package com.tsa.mqmonitor.config;

import com.tsa.mqmonitor.metrics.MqMeterBinder;
import com.tsa.mqmonitor.service.ConnectionService;
import com.tsa.mqmonitor.service.SamplerRegistry;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class MetricsConfig {

  @Bean
  MqMeterBinder mqMeterBinder(
      MeterRegistry registry, ConnectionService connections, SamplerRegistry samplers) {
    MqMeterBinder binder = new MqMeterBinder(registry, connections::findById);
    samplers.addListener(binder);
    samplers.addLifecycleListener(binder);
    return binder;
  }

  /** Prometheus shows MQ series only: nothing about the tool's own JVM, HTTP server or pools. */
  @Bean
  MeterFilter onlyIbmmqMeters() {
    return MeterFilter.deny(id -> !id.getName().startsWith("ibmmq."));
  }
}
