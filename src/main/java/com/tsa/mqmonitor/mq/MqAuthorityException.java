package com.tsa.mqmonitor.mq;

public class MqAuthorityException extends MqGatewayException {
  private final String objectName;

  public MqAuthorityException(String objectName) {
    this(objectName, "authority missing on " + objectName + " (MQRC 2035)");
  }

  public MqAuthorityException(String objectName, String message) {
    super(message);
    this.objectName = objectName;
  }

  public String objectName() {
    return objectName;
  }
}
