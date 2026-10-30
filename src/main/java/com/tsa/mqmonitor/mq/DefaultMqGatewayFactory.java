package com.tsa.mqmonitor.mq;

import org.springframework.stereotype.Component;

@Component
public class DefaultMqGatewayFactory implements MqGatewayFactory {
  @Override
  public MqGateway open(MqTarget target) {
    throw new UnsupportedOperationException("no MQ transport yet");
  }
}
