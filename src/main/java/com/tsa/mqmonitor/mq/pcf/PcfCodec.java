package com.tsa.mqmonitor.mq.pcf;

import com.ibm.mq.MQMessage;
import com.ibm.mq.constants.CMQCFC;
import com.ibm.mq.headers.MQDataException;
import com.ibm.mq.headers.pcf.PCFMessage;
import java.io.IOException;
import java.io.UncheckedIOException;

public final class PcfCodec {
  private PcfCodec() {}

  public static byte[] encode(PCFMessage pcf) {
    try {
      MQMessage m = new MQMessage();
      pcf.write(m);
      byte[] bytes = new byte[m.getMessageLength()];
      m.seek(0);
      m.readFully(bytes);
      return bytes;
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  public static PCFMessage decode(byte[] bytes) {
    try {
      MQMessage m = new MQMessage();
      m.write(bytes);
      m.seek(0);
      return new PCFMessage(m);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    } catch (MQDataException e) {
      throw new IllegalArgumentException("not a PCF message", e);
    }
  }

  public static boolean isLast(PCFMessage pcf) {
    return pcf.getControl() == CMQCFC.MQCFC_LAST;
  }
}
