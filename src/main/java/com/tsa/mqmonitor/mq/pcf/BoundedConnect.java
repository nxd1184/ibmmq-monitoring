package com.tsa.mqmonitor.mq.pcf;

import com.tsa.mqmonitor.mq.MqUnreachableException;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * Bounds an MQ client connect, which can block far longer than any configured timeout (for example
 * against a blackholed host), by running it on a daemon thread.
 */
final class BoundedConnect {
  /** A connect that hangs keeps its daemon thread until the OS gives up. */
  private static final ExecutorService CONNECTOR =
      Executors.newCachedThreadPool(
          r -> {
            Thread t = new Thread(r, "mq-connect");
            t.setDaemon(true);
            return t;
          });

  private BoundedConnect() {}

  /**
   * Runs {@code connect} and waits at most {@code timeout}. A timeout is a {@link
   * MqUnreachableException}; a connection that arrives after the caller gave up is passed to
   * {@code releaseLate}; a failure of {@code connect} is passed to {@code onFailure}.
   */
  static <T> T within(
      Duration timeout,
      Callable<T> connect,
      Consumer<T> releaseLate,
      Function<Throwable, RuntimeException> onFailure) {
    var result = new CompletableFuture<T>();
    CONNECTOR.execute(
        () -> {
          try {
            T c = connect.call();
            if (!result.complete(c)) releaseLate.accept(c);
          } catch (Throwable t) {
            result.completeExceptionally(t);
          }
        });
    try {
      return result.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      if (result.cancel(true)) {
        throw new MqUnreachableException(
            "connect timed out after " + timeout.toSeconds() + "s", e);
      }
      return result.join(); // completed in the race with the timeout
    } catch (ExecutionException e) {
      throw onFailure.apply(e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      result.cancel(true);
      throw new MqUnreachableException("connect interrupted", e);
    }
  }
}
