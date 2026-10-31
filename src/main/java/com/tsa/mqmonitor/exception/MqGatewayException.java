package com.tsa.mqmonitor.exception;

public class MqGatewayException extends RuntimeException {
  public MqGatewayException(String message) {
    super(message);
  }

  public MqGatewayException(String message, Throwable cause) {
    super(message, cause);
  }
}
