package com.tsa.mqmonitor.mq.pcf;

import com.tsa.mqmonitor.mq.BoundedCall;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Bounds an MQ client connect, which can block far longer than any configured timeout (for example
 * against a blackholed host), by running it on a daemon thread. See {@link BoundedCall}.
 */
final class BoundedConnect {
  private BoundedConnect() {}

  /**
   * Runs {@code connect} and waits at most {@code timeout}. A timeout is a {@link
   * com.tsa.mqmonitor.mq.MqUnreachableException}; a connection that arrives after the caller gave
   * up is passed to {@code releaseLate}; a failure of {@code connect}, including one that wins the
   * race with the timeout, is passed to {@code onFailure}.
   */
  static <T> T within(
      Duration timeout,
      Callable<T> connect,
      Consumer<T> releaseLate,
      Function<Throwable, RuntimeException> onFailure) {
    return BoundedCall.within(
        timeout,
        "connect timed out after " + BoundedCall.describe(timeout),
        connect,
        releaseLate,
        onFailure);
  }
}
