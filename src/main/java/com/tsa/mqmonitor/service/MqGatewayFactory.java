package com.tsa.mqmonitor.service;

import com.tsa.mqmonitor.model.MqTarget;

public interface MqGatewayFactory {
  MqGateway open(MqTarget target);
}
