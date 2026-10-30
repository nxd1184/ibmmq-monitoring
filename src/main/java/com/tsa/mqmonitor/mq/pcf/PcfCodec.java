package com.tsa.mqmonitor.mq.pcf;

import com.ibm.mq.MQMessage;
import com.ibm.mq.constants.CMQC;
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

  /** Decodes bytes written in the Java client's native encoding, as {@link #encode} produces. */
  public static PCFMessage decode(byte[] bytes) {
    return decode(bytes, CMQC.MQENC_NATIVE);
  }

  /**
   * Decodes bytes in the given MQ numeric encoding. Replies from the command server use the queue
   * manager's encoding (for example 546, little-endian, on Linux x86).
   */
  public static PCFMessage decode(byte[] bytes, int encoding) {
    try {
      MQMessage m = new MQMessage();
      m.encoding = encoding;
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
