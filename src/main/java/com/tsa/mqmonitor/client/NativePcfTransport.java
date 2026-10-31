package com.tsa.mqmonitor.client;

import com.ibm.mq.MQException;
import com.ibm.mq.MQQueueManager;
import com.ibm.mq.constants.CMQC;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.headers.MQDataException;
import com.ibm.mq.headers.pcf.PCFMessage;
import com.ibm.mq.headers.pcf.PCFMessageAgent;
import com.tsa.mqmonitor.exception.MqGatewayException;
import com.tsa.mqmonitor.model.MqTarget;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;
import javax.net.ssl.SSLSocketFactory;

/**
 * PCF over the IBM MQ classes for Java: one client connection plus a {@link PCFMessageAgent}. The
 * agent puts each command to {@code SYSTEM.ADMIN.COMMAND.QUEUE} and reads the replies from a
 * temporary dynamic queue; no application queue is ever opened.
 */
public final class NativePcfTransport implements PcfTransport {
  static final String APP_NAME = "mq-monitor";

  private final MQQueueManager qmgr;
  private final PCFMessageAgent agent;

  private NativePcfTransport(MQQueueManager qmgr, PCFMessageAgent agent) {
    this.qmgr = qmgr;
    this.agent = agent;
  }

  /**
   * Connects and opens the PCF agent (MQOPEN of the command queue and the reply model queue) as one
   * call bounded by the connect timeout: both steps block on the socket when the queue manager is
   * frozen.
   */
  public static NativePcfTransport connect(MqTarget target, SSLSocketFactory sslOrNull) {
    Duration timeout = target.connectTimeout();
    Hashtable<String, Object> props = properties(target, sslOrNull);
    return BoundedConnect.within(
        timeout,
        () -> open(target, props, timeout),
        NativePcfTransport::close,
        cause -> {
          if (cause instanceof MqGatewayException e) return e;
          if (cause instanceof MQException mq) {
            return PcfErrors.translateConnect(mq.reasonCode, target.queueManager(), mq);
          }
          return new MqGatewayException("connect failed: " + cause, cause);
        });
  }

  private static NativePcfTransport open(
      MqTarget target, Hashtable<String, Object> props, Duration timeout) throws MQException {
    MQQueueManager qmgr = new MQQueueManager(target.queueManager(), props);
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
      props.put(MQConstants.SSL_CIPHER_SUITE_PROPERTY, CipherSuites.forCipherSpec(target.tls().cipherSpec()));
      props.put(MQConstants.SSL_SOCKET_FACTORY_PROPERTY, ssl);
    }
    return props;
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
