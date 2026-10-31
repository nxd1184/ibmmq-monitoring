package com.tsa.mqmonitor.client;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.tsa.mqmonitor.exception.MqGatewayException;
import com.tsa.mqmonitor.exception.MqUnreachableException;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class BoundedConnectTest {
  @Test
  void returnsTheConnection() {
    assertThat(BoundedConnect.within(Duration.ofSeconds(1), () -> "c", c -> {}, BoundedConnectTest::fail))
        .isEqualTo("c");
  }

  @Test
  void failureIsTranslated() {
    assertThatThrownBy(() -> BoundedConnect.within(
            Duration.ofSeconds(1),
            () -> { throw new IllegalStateException("boom"); },
            c -> {},
            t -> new MqGatewayException("translated " + t.getMessage(), t)))
        .isInstanceOf(MqGatewayException.class)
        .hasMessage("translated boom");
  }

  @Test
  void timeoutIsUnreachableAndLateConnectionIsReleased() throws Exception {
    var proceed = new CountDownLatch(1);
    var released = new CountDownLatch(1);
    assertThatThrownBy(() -> BoundedConnect.within(
            Duration.ofMillis(100),
            () -> {
              awaitIgnoringInterrupts(proceed);
              return "late";
            },
            c -> released.countDown(),
            BoundedConnectTest::fail))
        .isInstanceOf(MqUnreachableException.class)
        .hasMessageContaining("timed out");
    proceed.countDown();
    assertThat(released.await(2, TimeUnit.SECONDS)).isTrue();
  }

  static RuntimeException fail(Throwable t) {
    return new MqGatewayException("failed", t);
  }

  /** A blocking socket connect does not react to interrupts either. */
  static void awaitIgnoringInterrupts(CountDownLatch latch) {
    while (true) {
      try {
        latch.await();
        return;
      } catch (InterruptedException e) {
        // keep blocking
      }
    }
  }
}
