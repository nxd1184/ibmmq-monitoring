package com.tsa.mqmonitor.mq.pcf;

import com.ibm.mq.constants.CMQC;
import com.ibm.mq.constants.MQConstants;
import com.tsa.mqmonitor.mq.MqAuthorityException;
import com.tsa.mqmonitor.mq.MqGatewayException;
import com.tsa.mqmonitor.mq.MqUnreachableException;
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

  private PcfErrors() {}

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
