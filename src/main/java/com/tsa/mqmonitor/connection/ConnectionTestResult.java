package com.tsa.mqmonitor.connection;

import com.tsa.mqmonitor.mq.QueueManagerInfo;

public record ConnectionTestResult(boolean ok, String message, QueueManagerInfo qmgr) {}
