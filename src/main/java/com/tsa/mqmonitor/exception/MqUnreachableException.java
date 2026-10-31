package com.tsa.mqmonitor.exception;

public class MqUnreachableException extends MqGatewayException {
  public MqUnreachableException(String message, Throwable cause) {
    super(message, cause);
  }
}
