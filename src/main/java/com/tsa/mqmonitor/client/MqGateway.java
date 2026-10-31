package com.tsa.mqmonitor.client;

import com.tsa.mqmonitor.model.ChannelInfo;
import com.tsa.mqmonitor.model.QueueInfo;
import com.tsa.mqmonitor.model.QueueManagerInfo;
import java.util.List;

public interface MqGateway extends AutoCloseable {
  /**
   * Starts a new per-sample deadline: the inquiries that follow, until the next call, share one
   * time budget (the connection's connect timeout). Gateways without a deadline ignore it.
   */
  default void beginSample() {}

  QueueManagerInfo inquireQueueManager();

  List<QueueInfo> inquireQueues(String filter, boolean hideSystemQueues);

  List<ChannelInfo> inquireChannels();

  @Override
  void close();
}
