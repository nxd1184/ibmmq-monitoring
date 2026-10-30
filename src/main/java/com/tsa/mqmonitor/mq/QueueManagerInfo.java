package com.tsa.mqmonitor.mq;

public record QueueManagerInfo(String name, String status, Integer commandLevel, String platform) {}
