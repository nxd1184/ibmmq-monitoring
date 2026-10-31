package com.tsa.mqmonitor.client;

import com.tsa.mqmonitor.model.MqTarget;

public interface MqGatewayFactory {
  MqGateway open(MqTarget target);
}
