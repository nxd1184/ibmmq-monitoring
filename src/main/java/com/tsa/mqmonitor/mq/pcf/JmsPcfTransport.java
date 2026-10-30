package com.tsa.mqmonitor.mq.pcf;

import com.ibm.mq.MQException;
import com.ibm.mq.constants.CMQC;
import com.ibm.mq.constants.CMQCFC;
import com.ibm.mq.constants.MQConstants;
import com.ibm.mq.headers.MQDataException;
import com.ibm.mq.headers.pcf.PCFException;
import com.ibm.mq.headers.pcf.PCFMessage;
import com.ibm.mq.jakarta.jms.MQConnectionFactory;
import com.ibm.msg.client.jakarta.wmq.WMQConstants;
import com.tsa.mqmonitor.mq.MqGatewayException;
import com.tsa.mqmonitor.mq.MqTarget;
import com.tsa.mqmonitor.mq.MqUnreachableException;
import com.tsa.mqmonitor.mq.tls.CipherSuites;
import jakarta.jms.BytesMessage;
import jakarta.jms.Connection;
import jakarta.jms.DeliveryMode;
import jakarta.jms.JMSException;
import jakarta.jms.Message;
import jakarta.jms.MessageConsumer;
import jakarta.jms.MessageProducer;
import jakarta.jms.Session;
import jakarta.jms.TemporaryQueue;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import javax.net.ssl.SSLSocketFactory;
import org.springframework.jms.support.JmsUtils;

/**
 * PCF over IBM MQ classes for Jakarta Messaging, the connection path that Spring JMS applications
 * use. JMS has no inquire API, so each PCF command goes as a {@link BytesMessage} (format {@code
 * MQADMIN}, no RFH2 header) to {@code SYSTEM.ADMIN.COMMAND.QUEUE}, and the command server's replies
 * are read from a temporary queue created from the reply model queue. No other queue is opened.
 *
 * <p>One connection, session, producer, temporary queue and consumer live as long as the
 * transport. Requests are serialized: one is outstanding at a time, and replies whose correlation
 * ID does not match the request's message ID (left over from a request that timed out) are
 * discarded.
 */
public final class JmsPcfTransport implements PcfTransport {
  static final String COMMAND_QUEUE_URI = "queue:///SYSTEM.ADMIN.COMMAND.QUEUE?targetClient=1";
  private static final String DEFAULT_MODEL = "SYSTEM.DEFAULT.MODEL.QUEUE";
  private static final Set<String> AUTH_ERROR_CODES = Set.of("JMSWMQ2013");
  private static final Set<String> CONNECT_ERROR_CODES = Set.of("JMSWMQ0018", "JMSWMQ0019");

  private final Connection connection;
  private final Session session;
  private final TemporaryQueue replyQueue;
  private final MessageProducer producer;
  private final MessageConsumer consumer;
  private final long timeoutMillis;
  private final String queueManager;
  private boolean closed;

  private JmsPcfTransport(
      Connection connection,
      Session session,
      TemporaryQueue replyQueue,
      MessageProducer producer,
      MessageConsumer consumer,
      long timeoutMillis,
      String queueManager) {
    this.connection = connection;
    this.session = session;
    this.replyQueue = replyQueue;
    this.producer = producer;
    this.consumer = consumer;
    this.timeoutMillis = timeoutMillis;
    this.queueManager = queueManager;
  }

  public static JmsPcfTransport connect(MqTarget target, SSLSocketFactory sslOrNull) {
    MQConnectionFactory factory;
    try {
      factory = connectionFactory(target, sslOrNull);
    } catch (JMSException e) {
      throw translate(e, target.queueManager());
    }
    Connection connection =
        BoundedConnect.within(
            target.connectTimeout(),
            factory::createConnection,
            JmsUtils::closeConnection,
            cause -> translate(cause, target.queueManager()));
    Session session = null;
    TemporaryQueue replyQueue = null;
    MessageProducer producer = null;
    try {
      session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
      // The temporary queue is created from the reply model queue.
      String model = isBlank(target.replyQueueModel()) ? DEFAULT_MODEL : target.replyQueueModel();
      try {
        replyQueue = session.createTemporaryQueue();
      } catch (JMSException e) {
        throw translate(e, model);
      }
      long timeoutMillis = target.connectTimeout().toMillis();
      producer = session.createProducer(session.createQueue(COMMAND_QUEUE_URI));
      producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
      producer.setTimeToLive(timeoutMillis);
      MessageConsumer consumer = session.createConsumer(replyQueue);
      connection.start();
      return new JmsPcfTransport(
          connection, session, replyQueue, producer, consumer, timeoutMillis, target.queueManager());
    } catch (JMSException e) {
      release(connection, session, replyQueue, producer, null);
      throw translate(e, target.queueManager());
    } catch (RuntimeException e) {
      release(connection, session, replyQueue, producer, null);
      throw e;
    }
  }

  /**
   * Sends one PCF request and returns every response message up to and including the last one.
   * Failed responses raise a {@link PCFException} exactly as {@link
   * com.ibm.mq.headers.pcf.PCFMessageAgent} does (see {@link #check}); transport failures are
   * unchecked {@link MqGatewayException}s.
   */
  @Override
  public synchronized List<PCFMessage> send(PCFMessage request) throws PCFException {
    if (closed) throw new IllegalStateException("transport is closed");
    try {
      BytesMessage message = session.createBytesMessage();
      message.writeBytes(PcfCodec.encode(request));
      message.setIntProperty(WMQConstants.JMS_IBM_ENCODING, CMQC.MQENC_NATIVE); // as PcfCodec.encode writes
      message.setStringProperty(WMQConstants.JMS_IBM_FORMAT, MQConstants.MQFMT_ADMIN);
      message.setIntProperty(WMQConstants.JMS_IBM_MSGTYPE, MQConstants.MQMT_REQUEST);
      message.setJMSReplyTo(replyQueue);
      producer.send(message);
      // The command server copies the request MsgId into each reply's CorrelId.
      String requestId = message.getJMSMessageID();

      List<PCFMessage> responses = new ArrayList<>();
      while (true) {
        Message reply = consumer.receive(timeoutMillis);
        if (reply == null) {
          throw new MqUnreachableException(
              "no PCF reply within " + (timeoutMillis / 1000) + "s", null);
        }
        if (!Objects.equals(requestId, reply.getJMSCorrelationID())) continue; // stale reply
        if (!(reply instanceof BytesMessage bytes)) {
          throw new MqGatewayException(
              "unexpected PCF reply type " + reply.getClass().getSimpleName());
        }
        byte[] body = new byte[(int) bytes.getBodyLength()];
        bytes.readBytes(body);
        // The command server replies in the queue manager's encoding, not the request's.
        PCFMessage pcf = PcfCodec.decode(body, bytes.getIntProperty(WMQConstants.JMS_IBM_ENCODING));
        responses.add(pcf);
        if (PcfCodec.isLast(pcf)) return check(responses);
      }
    } catch (JMSException e) {
      throw translate(e, queueManager);
    }
  }

  @Override
  public synchronized void close() {
    if (closed) return;
    closed = true;
    release(connection, session, replyQueue, producer, consumer);
  }

  /**
   * The command server follows failed responses with a summary {@code MQRCCF_COMMAND_FAILED}
   * (3008); the error is then reported with the first other reason, as {@code PCFMessageAgent}
   * does. A single failed response is reported with its own reason.
   */
  static List<PCFMessage> check(List<PCFMessage> responses) throws PCFException {
    boolean commandFailed = false;
    int reason = CMQCFC.MQRCCF_COMMAND_FAILED;
    for (PCFMessage m : responses) {
      if (m.getCompCode() != CMQC.MQCC_FAILED) continue;
      if (m.getReason() == CMQCFC.MQRCCF_COMMAND_FAILED) {
        commandFailed = true;
      } else if (reason == CMQCFC.MQRCCF_COMMAND_FAILED) {
        reason = m.getReason();
      }
    }
    PCFMessage[] source = responses.toArray(PCFMessage[]::new);
    if (commandFailed) throw new PCFException(CMQC.MQCC_FAILED, reason, source);
    if (responses.size() == 1 && responses.get(0).getCompCode() == CMQC.MQCC_FAILED) {
      throw new PCFException(CMQC.MQCC_FAILED, responses.get(0).getReason(), source);
    }
    return responses;
  }

  static MQConnectionFactory connectionFactory(MqTarget target, SSLSocketFactory ssl)
      throws JMSException {
    var factory = new MQConnectionFactory();
    factory.setTransportType(WMQConstants.WMQ_CM_CLIENT);
    factory.setHostName(target.host());
    factory.setPort(target.port());
    factory.setChannel(target.channel());
    factory.setQueueManager(target.queueManager());
    factory.setAppName(NativePcfTransport.APP_NAME);
    factory.setClientReconnectOptions(WMQConstants.WMQ_CLIENT_RECONNECT_DISABLED);
    if (!isBlank(target.replyQueueModel())) factory.setTemporaryModel(target.replyQueueModel());
    if (!isBlank(target.username())) {
      factory.setStringProperty(WMQConstants.USERID, target.username());
      factory.setStringProperty(
          WMQConstants.PASSWORD, target.password() == null ? "" : target.password());
      factory.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, true);
    }
    if (ssl != null) {
      factory.setSSLCipherSuite(CipherSuites.forCipherSpec(target.tls().cipherSpec()));
      factory.setSSLSocketFactory(ssl);
    }
    return factory;
  }

  /**
   * Uses the MQ reason code linked to the JMS exception; without one, falls back to the IBM JMS
   * error code.
   */
  static MqGatewayException translate(Throwable failure, String objectName) {
    if (failure instanceof MqGatewayException e) return e;
    Throwable t = failure;
    for (int depth = 0; t != null && depth < 16; depth++) {
      if (t instanceof MQException mq) return PcfErrors.translate(mq.reasonCode, objectName, failure);
      if (t instanceof MQDataException mq) return PcfErrors.translate(mq.reasonCode, objectName, failure);
      t = next(t);
    }
    if (failure instanceof JMSException jms && jms.getErrorCode() != null) {
      if (AUTH_ERROR_CODES.contains(jms.getErrorCode())) {
        return PcfErrors.translate(CMQC.MQRC_NOT_AUTHORIZED, objectName, failure);
      }
      if (CONNECT_ERROR_CODES.contains(jms.getErrorCode())) {
        return new MqUnreachableException("connect failed: " + jms.getMessage(), failure);
      }
    }
    return new MqGatewayException("JMS failure: " + failure.getMessage(), failure);
  }

  private static Throwable next(Throwable t) {
    if (t.getCause() != null) return t.getCause();
    return t instanceof JMSException jms ? jms.getLinkedException() : null;
  }

  private static void release(
      Connection connection,
      Session session,
      TemporaryQueue replyQueue,
      MessageProducer producer,
      MessageConsumer consumer) {
    JmsUtils.closeMessageConsumer(consumer);
    JmsUtils.closeMessageProducer(producer);
    if (replyQueue != null) {
      try {
        replyQueue.delete();
      } catch (JMSException | RuntimeException e) {
        // closing the connection removes it as well
      }
    }
    JmsUtils.closeSession(session);
    JmsUtils.closeConnection(connection);
  }

  private static boolean isBlank(String s) {
    return s == null || s.isBlank();
  }
}
