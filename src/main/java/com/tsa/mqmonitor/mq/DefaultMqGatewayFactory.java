package com.tsa.mqmonitor.mq;

import com.tsa.mqmonitor.config.MqMonitorProperties;
import com.tsa.mqmonitor.mq.pcf.NativePcfTransport;
import com.tsa.mqmonitor.mq.pcf.PcfMqGateway;
import com.tsa.mqmonitor.mq.tls.SslContextFactory;
import java.time.Duration;
import org.springframework.stereotype.Component;

@Component
public class DefaultMqGatewayFactory implements MqGatewayFactory {
  private final Duration defaultConnectTimeout;

  public DefaultMqGatewayFactory(MqMonitorProperties props) {
    this.defaultConnectTimeout = Duration.ofSeconds(props.mq().connectTimeoutSeconds());
  }

  @Override
  public MqGateway open(MqTarget target) {
    MqTarget t = target.connectTimeout() != null ? target : withDefaultTimeout(target);
    return switch (t.clientType()) {
      case NATIVE -> new PcfMqGateway(NativePcfTransport.connect(t, SslContextFactory.build(t.tls())));
      case SPRING_JMS -> throw new UnsupportedOperationException("SPRING_JMS transport arrives in Task 7");
    };
  }

  private MqTarget withDefaultTimeout(MqTarget t) {
    return new MqTarget(
        t.host(), t.port(), t.queueManager(), t.channel(), t.username(), t.password(),
        t.clientType(), t.tls(), t.replyQueueModel(), defaultConnectTimeout);
  }
}
