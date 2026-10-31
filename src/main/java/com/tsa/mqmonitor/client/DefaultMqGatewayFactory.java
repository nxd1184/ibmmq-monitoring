package com.tsa.mqmonitor.client;

import com.tsa.mqmonitor.config.MqMonitorProperties;
import com.tsa.mqmonitor.model.MqTarget;
import java.time.Duration;
import org.springframework.stereotype.Component;

@Component
public class DefaultMqGatewayFactory implements MqGatewayFactory {
  private final Duration defaultConnectTimeout;

  public DefaultMqGatewayFactory(MqMonitorProperties mqMonitorProperties) {
    this.defaultConnectTimeout =
        Duration.ofSeconds(mqMonitorProperties.mq().connectTimeoutSeconds());
  }

  @Override
  public MqGateway open(MqTarget target) {
    MqTarget t = target.connectTimeout() != null ? target : withDefaultTimeout(target);
    return switch (t.clientType()) {
      case NATIVE ->
          new PcfMqGateway(
              NativePcfTransport.connect(t, SslContextFactory.build(t.tls())), t.connectTimeout());
      case SPRING_JMS ->
          new PcfMqGateway(
              JmsPcfTransport.connect(t, SslContextFactory.build(t.tls())), t.connectTimeout());
    };
  }

  private MqTarget withDefaultTimeout(MqTarget t) {
    return new MqTarget(
        t.host(), t.port(), t.queueManager(), t.channel(), t.username(), t.password(),
        t.clientType(), t.tls(), t.replyQueueModel(), defaultConnectTimeout);
  }
}
