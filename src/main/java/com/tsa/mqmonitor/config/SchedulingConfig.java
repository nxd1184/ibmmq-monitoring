package com.tsa.mqmonitor.config;

import com.tsa.mqmonitor.service.impl.HealthEvaluator;
import java.time.Clock;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

@Configuration
public class SchedulingConfig {

  @Bean(name = "samplerScheduler")
  public ThreadPoolTaskScheduler samplerScheduler() {
    var scheduler = new ThreadPoolTaskScheduler();
    scheduler.setPoolSize(8);
    scheduler.setThreadNamePrefix("mq-sampler-");
    scheduler.setWaitForTasksToCompleteOnShutdown(false);
    scheduler.setRemoveOnCancelPolicy(true);
    return scheduler;
  }

  @Bean
  @ConditionalOnMissingBean
  public Clock clock() {
    return Clock.systemUTC();
  }

  @Bean
  @ConditionalOnMissingBean
  public HealthEvaluator healthEvaluator() {
    return new HealthEvaluator();
  }
}
