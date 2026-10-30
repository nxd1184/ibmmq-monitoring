package com.tsa.mqmonitor.sampling;

/** No connection row exists for the id; a web caller maps it to 404. */
public class UnknownConnectionException extends RuntimeException {
  public UnknownConnectionException(long id) {
    super("Unknown connection " + id);
  }
}
