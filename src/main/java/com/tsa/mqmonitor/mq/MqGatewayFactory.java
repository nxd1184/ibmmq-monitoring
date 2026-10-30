package com.tsa.mqmonitor.mq;

public interface MqGatewayFactory {
  MqGateway open(MqTarget target);
}
