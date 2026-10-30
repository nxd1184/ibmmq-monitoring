package com.tsa.mqmonitor.mq;

public class MqUnreachableException extends MqGatewayException {
  public MqUnreachableException(String message, Throwable cause) {
    super(message, cause);
  }
}
