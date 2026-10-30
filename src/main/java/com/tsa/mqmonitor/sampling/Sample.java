package com.tsa.mqmonitor.sampling;

import com.tsa.mqmonitor.mq.ChannelInfo;
import com.tsa.mqmonitor.mq.QueueInfo;
import com.tsa.mqmonitor.mq.QueueManagerInfo;
import java.time.Instant;
import java.util.List;

public record Sample(
    long connectionId,
    Instant takenAt,
    QueueManagerInfo qmgr,
    List<QueueInfo> queues,
    List<ChannelInfo> channels,
    String error) {

  public static Sample failed(long id, Instant at, String error) {
    return new Sample(id, at, null, List.of(), List.of(), error);
  }

  public boolean reachable() {
    return error == null;
  }
}
