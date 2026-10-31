package com.tsa.mqmonitor.model;

public record ConnectionTestResult(boolean ok, String message, QueueManagerInfo qmgr) {}
