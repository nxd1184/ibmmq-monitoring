package com.tsa.mqmonitor.mq.pcf;

import com.ibm.mq.MQException;
import com.ibm.mq.headers.MQDataException;
import com.ibm.mq.headers.pcf.PCFMessage;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class FakeTransport implements PcfTransport {
  final List<PCFMessage> requests = new ArrayList<>();
  final Map<Integer, List<PCFMessage>> responses = new HashMap<>();
  final Map<Integer, Exception> failures = new HashMap<>();
  boolean closed;

  void respond(int command, PCFMessage... messages) {
    responses.put(command, List.of(messages));
  }

  void fail(int command, Exception e) {
    failures.put(command, e);
  }

  @Override
  public List<PCFMessage> send(PCFMessage request) throws MQException, MQDataException, IOException {
    requests.add(request);
    Exception e = failures.get(request.getCommand());
    if (e instanceof MQException m) throw m;
    if (e instanceof MQDataException d) throw d;
    if (e instanceof IOException io) throw io;
    return responses.getOrDefault(request.getCommand(), List.of());
  }

  @Override
  public void close() {
    closed = true;
  }
}
