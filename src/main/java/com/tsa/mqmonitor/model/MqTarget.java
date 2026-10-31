package com.tsa.mqmonitor.model;

import com.tsa.mqmonitor.util.Redact;
import java.time.Duration;

public record MqTarget(
    String host,
    int port,
    String queueManager,
    String channel,
    String username,
    String password,
    ClientType clientType,
    TlsSettings tls,
    String replyQueueModel,
    Duration connectTimeout) {

  /** Never prints the password. */
  @Override
  public String toString() {
    return "MqTarget[host=" + host + ", port=" + port + ", queueManager=" + queueManager
        + ", channel=" + channel + ", username=" + username + ", password=" + Redact.secret(password)
        + ", clientType=" + clientType + ", tls=" + tls + ", replyQueueModel=" + replyQueueModel
        + ", connectTimeout=" + connectTimeout + "]";
  }
}
