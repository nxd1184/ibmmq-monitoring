package com.tsa.mqmonitor.service.impl;

import com.tsa.mqmonitor.config.MqMonitorProperties;
import com.tsa.mqmonitor.enumerable.ClientType;
import com.tsa.mqmonitor.enumerable.TlsMode;
import com.tsa.mqmonitor.exception.MqGatewayException;
import com.tsa.mqmonitor.model.ConnectionForm;
import com.tsa.mqmonitor.model.ConnectionTestResult;
import com.tsa.mqmonitor.model.MqConnection;
import com.tsa.mqmonitor.model.MqTarget;
import com.tsa.mqmonitor.model.Thresholds;
import com.tsa.mqmonitor.model.TlsSettings;
import com.tsa.mqmonitor.repository.MqConnectionRepository;
import com.tsa.mqmonitor.security.SecretCipher;
import com.tsa.mqmonitor.service.MqGateway;
import com.tsa.mqmonitor.service.MqGatewayFactory;
import com.tsa.mqmonitor.util.BoundedCall;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.dao.DataAccessException;
import org.springframework.data.relational.core.conversion.DbActionExecutionException;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
public class ConnectionService {
  private static final Logger log = LoggerFactory.getLogger(ConnectionService.class);

  private final MqConnectionRepository mqConnectionRepository;
  private final SecretCipher secretCipher;
  private final MqMonitorProperties mqMonitorProperties;
  private final MqGatewayFactory mqGatewayFactory;
  private final ObjectProvider<SamplerRegistry> samplerRegistry;

  public ConnectionService(
      MqConnectionRepository mqConnectionRepository,
      SecretCipher secretCipher,
      MqMonitorProperties mqMonitorProperties,
      MqGatewayFactory mqGatewayFactory,
      ObjectProvider<SamplerRegistry> samplerRegistry) {
    this.mqConnectionRepository = mqConnectionRepository;
    this.secretCipher = secretCipher;
    this.mqMonitorProperties = mqMonitorProperties;
    this.mqGatewayFactory = mqGatewayFactory;
    this.samplerRegistry = samplerRegistry;
  }

  public List<MqConnection> findAll() {
    return mqConnectionRepository.findAll();
  }

  public Optional<MqConnection> findById(long id) {
    return mqConnectionRepository.findById(id);
  }

  public List<MqConnection> findPrometheusExports() {
    return mqConnectionRepository.findByPrometheusExportTrue();
  }

  public void delete(long id) {
    mqConnectionRepository.deleteById(id);
    samplerRegistry.ifAvailable(r -> r.connectionDeleted(id));
  }

  public MqConnection save(ConnectionForm form) {
    Optional<MqConnection> existing = existingFor(form);
    String passwordEncrypted =
        StringUtils.hasText(form.getPassword())
            ? secretCipher.encrypt(form.getPassword())
            : existing.map(MqConnection::passwordEncrypted).orElse(null);
    Instant now = Instant.now();
    Instant createdAt = existing.map(MqConnection::createdAt).orElse(now);
    var entity =
        new MqConnection(
            form.getId(),
            form.getName(),
            form.getHost(),
            form.getPort(),
            form.getQueueManager(),
            form.getChannel(),
            form.getUsername(),
            passwordEncrypted,
            form.getClientType(),
            form.getTlsMode(),
            form.getCipherSpec(),
            form.getTruststorePath(),
            form.getKeystorePath(),
            form.getCertificateLabel(),
            normalizeQueueFilter(form.getQueueFilter()),
            form.isHideSystemQueues(),
            form.getSampleIntervalSeconds(),
            form.isPrometheusExport(),
            form.getReplyQueueModel(),
            form.getThresholdDepthMax(),
            form.getThresholdDepthPercent(),
            form.getThresholdNoConsumerSeconds(),
            form.getThresholdOldestMessageAgeSeconds(),
            createdAt,
            now);
    MqConnection saved;
    try {
      saved = mqConnectionRepository.save(entity);
    } catch (DbActionExecutionException e) {
      if (e.getCause() instanceof DataAccessException dae) {
        throw dae;
      }
      throw e;
    }
    samplerRegistry.ifAvailable(r -> r.connectionChanged(saved.id()));
    return saved;
  }

  /** A blank queue filter is stored as {@code *} (all queues). */
  public static String normalizeQueueFilter(String filter) {
    return StringUtils.hasText(filter) ? filter.trim() : "*";
  }

  public ConnectionForm toForm(MqConnection c) {
    var f = new ConnectionForm();
    f.setId(c.id());
    f.setName(c.name());
    f.setHost(c.host());
    f.setPort(c.port());
    f.setQueueManager(c.queueManager());
    f.setChannel(c.channel());
    f.setUsername(c.username());
    f.setClientType(c.clientType());
    f.setTlsMode(c.tlsMode());
    f.setCipherSpec(c.cipherSpec());
    f.setTruststorePath(c.truststorePath());
    f.setKeystorePath(c.keystorePath());
    f.setCertificateLabel(c.certificateLabel());
    f.setQueueFilter(c.queueFilter());
    f.setHideSystemQueues(c.hideSystemQueues());
    f.setSampleIntervalSeconds(c.sampleIntervalSeconds());
    f.setPrometheusExport(c.prometheusExport());
    f.setReplyQueueModel(c.replyQueueModel());
    f.setThresholdDepthMax(c.thresholdDepthMax());
    f.setThresholdDepthPercent(c.thresholdDepthPercent());
    f.setThresholdNoConsumerSeconds(c.thresholdNoConsumerSeconds());
    f.setThresholdOldestMessageAgeSeconds(c.thresholdOldestMessageAgeSeconds());
    return f;
  }

  public String plainPassword(MqConnection c) {
    return secretCipher.decrypt(c.passwordEncrypted());
  }

  public Thresholds thresholdsFor(MqConnection c) {
    var t = mqMonitorProperties.thresholds();
    return new Thresholds(
            t.depthMax(), t.depthPercent(), t.noConsumerSeconds(), t.oldestMessageAgeSeconds())
        .withOverrides(
            c.thresholdDepthMax(),
            c.thresholdDepthPercent(),
            c.thresholdNoConsumerSeconds(),
            c.thresholdOldestMessageAgeSeconds());
  }

  public MqTarget targetFor(ConnectionForm f) {
    String password =
        StringUtils.hasText(f.getPassword())
            ? f.getPassword()
            : existingFor(f).map(this::plainPassword).orElse(null);
    return target(
        f.getHost(),
        f.getPort(),
        f.getQueueManager(),
        f.getChannel(),
        f.getUsername(),
        password,
        f.getClientType(),
        tlsFor(
            f.getTlsMode(),
            f.getCipherSpec(),
            f.getTruststorePath(),
            f.getKeystorePath(),
            f.getCertificateLabel()),
        f.getReplyQueueModel());
  }

  public MqTarget targetFor(MqConnection c) {
    return target(
        c.host(),
        c.port(),
        c.queueManager(),
        c.channel(),
        c.username(),
        plainPassword(c),
        c.clientType(),
        tlsFor(
            c.tlsMode(),
            c.cipherSpec(),
            c.truststorePath(),
            c.keystorePath(),
            c.certificateLabel()),
        c.replyQueueModel());
  }

  public ConnectionTestResult test(ConnectionForm form) {
    MqGateway gateway = null;
    try {
      gateway = mqGatewayFactory.open(targetFor(form));
      gateway.beginSample();
      var qmgr = gateway.inquireQueueManager();
      return new ConnectionTestResult(true, "Connected to " + qmgr.name(), qmgr);
    } catch (MqGatewayException e) {
      return new ConnectionTestResult(false, e.getMessage(), null);
    } catch (RuntimeException e) {
      // e.g. an unreadable truststore or a stored password the current key cannot decrypt
      log.warn("Connection test failed: {}", e.toString());
      return new ConnectionTestResult(
          false, e.getMessage() != null ? e.getMessage() : e.getClass().getSimpleName(), null);
    } finally {
      if (gateway != null) {
        MqGateway gw = gateway;
        BoundedCall.runAsync("closing test connection", gw::close);
      }
    }
  }

  private Optional<MqConnection> existingFor(ConnectionForm form) {
    return form.getId() == null ? Optional.empty() : mqConnectionRepository.findById(form.getId());
  }

  private MqTarget target(
      String host,
      int port,
      String queueManager,
      String channel,
      String username,
      String password,
      ClientType clientType,
      TlsSettings tls,
      String replyQueueModel) {
    return new MqTarget(
        host,
        port,
        queueManager,
        channel,
        username,
        password,
        clientType,
        tls,
        replyQueueModel,
        Duration.ofSeconds(mqMonitorProperties.mq().connectTimeoutSeconds()));
  }

  private TlsSettings tlsFor(
      TlsMode mode, String cipherSpec, String truststore, String keystore, String label) {
    if (mode == null || mode == TlsMode.NONE) {
      return TlsSettings.none();
    }
    var d = mqMonitorProperties.tls();
    return new TlsSettings(
        mode,
        cipherSpec,
        StringUtils.hasText(truststore) ? truststore : d.truststorePath(),
        d.truststorePassword(),
        StringUtils.hasText(keystore) ? keystore : d.keystorePath(),
        d.keystorePassword(),
        label);
  }
}
