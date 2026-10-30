package com.tsa.mqmonitor.mq;

import java.util.List;

public interface MqGateway extends AutoCloseable {
  QueueManagerInfo inquireQueueManager();

  List<QueueInfo> inquireQueues(String filter, boolean hideSystemQueues);

  List<ChannelInfo> inquireChannels();

  @Override
  void close();
}
