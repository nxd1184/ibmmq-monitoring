package com.tsa.mqmonitor.service;

import com.ibm.mq.MQException;
import com.ibm.mq.headers.MQDataException;
import com.ibm.mq.headers.pcf.PCFMessage;
import java.io.IOException;
import java.util.List;

/** Carries one PCF request to the queue manager and returns every response message. */
public interface PcfTransport extends AutoCloseable {
  /**
   * Sends the request and returns the complete, already reassembled list of response messages.
   * {@code PCFException} is a {@link MQDataException}.
   */
  List<PCFMessage> send(PCFMessage request) throws MQException, MQDataException, IOException;

  @Override
  void close();
}
