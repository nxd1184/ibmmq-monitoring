package com.tsa.mqmonitor.mq.pcf;

import com.ibm.mq.MQException;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.CMQC;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.headers.MQDataException;
import com.ibm.mq.headers.pcf.PCFMessage;
import com.ibm.mq.headers.pcf.PCFMessageAgent;
import com.ibm.mq.jmqi.CipherSuiteUtils;
import com.tsa.mqmonitor.mq.MqGatewayException;
import com.tsa.mqmonitor.mq.MqTarget;
import com.tsa.mqmonitor.mq.MqUnreachableException;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLSocketFactory;

/**
 * PCF over the IBM MQ classes for Java: one client connection plus a {@link PCFMessageAgent}. The
 * agent puts each command to {@code SYSTEM.ADMIN.COMMAND.QUEUE} and reads the replies from a
 * temporary dynamic queue; no application queue is ever opened.
 */
public final class NativePcfTransport implements PcfTransport {
  static final String APP_NAME = "mq-monitor";

  /** A connect that hangs (e.g. a blackholed host) keeps its daemon thread until the OS gives up. */
  private static final ExecutorService CONNECTOR =
      Executors.newCachedThreadPool(
          r -> {
            Thread t = new Thread(r, "mq-connect");
            t.setDaemon(true);
            return t;
          });

  private final MQQueueManager qmgr;
  private final PCFMessageAgent agent;

  private NativePcfTransport(MQQueueManager qmgr, PCFMessageAgent agent) {
    this.qmgr = qmgr;
    this.agent = agent;
  }

  public static NativePcfTransport connect(MqTarget target, SSLSocketFactory sslOrNull) {
    Duration timeout = target.connectTimeout();
    MQQueueManager qmgr = connectQueueManager(target, properties(target, sslOrNull), timeout);
    try {
      var agent = new PCFMessageAgent();
      if (!isBlank(target.replyQueueModel())) agent.setModelQueueName(target.replyQueueModel());
      // PCFAgent takes seconds and converts to milliseconds itself.
      agent.setWaitInterval((int) Math.max(1, timeout.toSeconds()));
      agent.connect(qmgr);
      return new NativePcfTransport(qmgr, agent);
    } catch (MQDataException e) {
      disconnectQuietly(qmgr);
      throw PcfErrors.translate(e.reasonCode, target.replyQueueModel(), e);
    } catch (RuntimeException e) {
      disconnectQuietly(qmgr);
      throw e;
    }
  }

  @Override
  public List<PCFMessage> send(PCFMessage request) throws MQException, MQDataException, IOException {
    return Arrays.asList(agent.send(request));
  }

  @Override
  public void close() {
    try {
      agent.disconnect();
    } catch (MQDataException | RuntimeException e) {
      // already broken; nothing to release
    }
    disconnectQuietly(qmgr);
  }

  static Hashtable<String, Object> properties(MqTarget target, SSLSocketFactory ssl) {
    var props = new Hashtable<String, Object>();
    props.put(MQConstants.TRANSPORT_PROPERTY, MQConstants.TRANSPORT_MQSERIES_CLIENT);
    props.put(MQConstants.HOST_NAME_PROPERTY, target.host());
    props.put(MQConstants.PORT_PROPERTY, target.port());
    props.put(MQConstants.CHANNEL_PROPERTY, target.channel());
    props.put(MQConstants.APPNAME_PROPERTY, APP_NAME);
    props.put(MQConstants.CONNECT_OPTIONS_PROPERTY, CMQC.MQCNO_RECONNECT_DISABLED);
    if (!isBlank(target.username())) {
      props.put(MQConstants.USER_ID_PROPERTY, target.username());
      props.put(MQConstants.PASSWORD_PROPERTY, target.password() == null ? "" : target.password());
      props.put(MQConstants.USE_MQCSP_AUTHENTICATION_PROPERTY, true);
    }
    if (ssl != null) {
      props.put(MQConstants.SSL_CIPHER_SUITE_PROPERTY, cipherSuite(target.tls().cipherSpec()));
      props.put(MQConstants.SSL_SOCKET_FACTORY_PROPERTY, ssl);
    }
    return props;
  }

  /**
   * The MQ classes for Java take a CipherSuite, not the CipherSpec an MQ admin configures (for
   * example {@code ANY_TLS12_OR_HIGHER} becomes {@code *TLS12ORHIGHER}). A value that is not a
   * known CipherSpec is passed through as a CipherSuite name.
   */
  static String cipherSuite(String cipherSpec) {
    String suite = CipherSuiteUtils.toCipherSuite(cipherSpec);
    return suite != null ? suite : cipherSpec;
  }

  private static MQQueueManager connectQueueManager(
      MqTarget target, Hashtable<String, Object> props, Duration timeout) {
    var result = new CompletableFuture<MQQueueManager>();
    CONNECTOR.execute(
        () -> {
          try {
            MQQueueManager q = new MQQueueManager(target.queueManager(), props);
            // The caller gave up already: release the connection nobody will close.
            if (!result.complete(q)) disconnectQuietly(q);
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
      if (e.getCause() instanceof MQException mq) {
        throw PcfErrors.translate(mq.reasonCode, target.queueManager(), mq);
      }
      throw new MqGatewayException("connect failed: " + e.getCause(), e.getCause());
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      result.cancel(true);
      throw new MqUnreachableException("connect interrupted", e);
    }
  }

  private static void disconnectQuietly(MQQueueManager q) {
    try {
      q.disconnect();
    } catch (MQException | RuntimeException e) {
      // best effort
    }
  }

  private static boolean isBlank(String s) {
    return s == null || s.isBlank();
  }
}
