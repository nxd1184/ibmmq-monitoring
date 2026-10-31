package com.tsa.mqmonitor.util;

import com.tsa.mqmonitor.exception.MqUnreachableException;
import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bounds MQ client calls that can block far longer than any configured timeout (a connect to a
 * blackholed host, a PCF exchange with a frozen queue manager) by running them on a daemon thread.
 * The same daemon pool closes connections asynchronously, because a close on a dead socket can
 * block as well.
 */
public final class BoundedCall {
  private static final Logger log = LoggerFactory.getLogger(BoundedCall.class);

  /** A call that hangs keeps its daemon thread until the OS gives up on the socket. */
  private static final ExecutorService POOL =
      Executors.newCachedThreadPool(
          new java.util.concurrent.ThreadFactory() {
            private final AtomicInteger n = new AtomicInteger();

            @Override
            public Thread newThread(Runnable r) {
              Thread t = new Thread(r, "mq-io-" + n.incrementAndGet());
              t.setDaemon(true);
              return t;
            }
          });

  private BoundedCall() {}

  /**
   * Runs {@code call} and waits at most {@code timeout}. A timeout is a {@link
   * MqUnreachableException} with {@code timeoutMessage}; a result that arrives after the caller
   * gave up is passed to {@code releaseLate}; a failure of {@code call} is passed to {@code
   * onFailure}.
   */
  public static <T> T within(
      Duration timeout,
      String timeoutMessage,
      Callable<T> call,
      Consumer<T> releaseLate,
      Function<Throwable, RuntimeException> onFailure) {
    var result = new CompletableFuture<T>();
    POOL.execute(
        () -> {
          try {
            T c = call.call();
            if (!result.complete(c) && c != null) releaseLate.accept(c);
          } catch (Throwable t) {
            result.completeExceptionally(t);
          }
        });
    try {
      return result.get(Math.max(1, timeout.toMillis()), TimeUnit.MILLISECONDS);
    } catch (TimeoutException e) {
      if (result.cancel(true)) {
        throw new MqUnreachableException(timeoutMessage, e);
      }
      return joinCompleted(result, onFailure); // completed in the race with the timeout
    } catch (ExecutionException e) {
      throw onFailure.apply(e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      result.cancel(true);
      throw new MqUnreachableException("interrupted while waiting for MQ", e);
    }
  }

  /** Runs {@code task} on the daemon pool; failures are logged at debug and otherwise ignored. */
  public static void runAsync(String what, Runnable task) {
    try {
      POOL.execute(
          () -> {
            try {
              task.run();
            } catch (RuntimeException e) {
              log.debug("{} failed: {}", what, e.toString());
            }
          });
    } catch (RejectedExecutionException e) {
      log.debug("{} skipped: {}", what, e.toString());
    }
  }

  /** Formats a timeout for an operator-facing message: whole seconds, or milliseconds below 1s. */
  public static String describe(Duration timeout) {
    return timeout.toMillis() >= 1000 ? timeout.toSeconds() + "s" : timeout.toMillis() + "ms";
  }

  private static <T> T joinCompleted(
      CompletableFuture<T> result, Function<Throwable, RuntimeException> onFailure) {
    try {
      return result.join();
    } catch (CompletionException e) {
      throw onFailure.apply(e.getCause() != null ? e.getCause() : e);
    }
  }
}
