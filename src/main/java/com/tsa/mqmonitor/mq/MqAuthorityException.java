package com.tsa.mqmonitor.mq;

public class MqAuthorityException extends MqGatewayException {
  private final String objectName;

  public MqAuthorityException(String objectName) {
    super("authority missing on " + objectName + " (MQRC 2035)");
    this.objectName = objectName;
  }

  public String objectName() {
    return objectName;
  }
}
