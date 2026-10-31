package com.tsa.mqmonitor.service.impl;

import com.ibm.mq.constants.CMQC;
import com.ibm.mq.constants.MQConstants;
import com.tsa.mqmonitor.exception.MqAuthorityException;
import com.tsa.mqmonitor.exception.MqGatewayException;
import com.tsa.mqmonitor.exception.MqUnreachableException;
import java.util.Set;

public final class PcfErrors {
  private static final Set<Integer> UNREACHABLE =
      Set.of(
          CMQC.MQRC_HOST_NOT_AVAILABLE,
          CMQC.MQRC_Q_MGR_NOT_AVAILABLE,
          CMQC.MQRC_CONNECTION_BROKEN,
          CMQC.MQRC_CHANNEL_NOT_AVAILABLE,
          CMQC.MQRC_UNKNOWN_CHANNEL_NAME,
          CMQC.MQRC_SSL_INITIALIZATION_ERROR);

  static final String CONNECT_NOT_AUTHORIZED =
      "not authorized: check username/password or +connect authority (MQRC 2035)";

  private PcfErrors() {}

  /**
   * As {@link #translate}, for a failed connect: MQRC 2035 there almost always means bad
   * credentials or a missing {@code +connect} authority, not a missing object authority.
   */
  public static MqGatewayException translateConnect(int reason, String queueManager, Throwable cause) {
    if (reason == CMQC.MQRC_NOT_AUTHORIZED) {
      var e = new MqAuthorityException(queueManager, CONNECT_NOT_AUTHORIZED);
      if (cause != null) e.initCause(cause);
      return e;
    }
    return translate(reason, queueManager, cause);
  }

  public static MqGatewayException translate(int reason, String objectName, Throwable cause) {
    if (reason == CMQC.MQRC_NOT_AUTHORIZED) {
      var e = new MqAuthorityException(objectName);
      if (cause != null) e.initCause(cause);
      return e;
    }
    String text = "MQRC " + reason + " " + MQConstants.lookupReasonCode(reason);
    if (UNREACHABLE.contains(reason)) return new MqUnreachableException(text, cause);
    return new MqGatewayException(text, cause);
  }
}
