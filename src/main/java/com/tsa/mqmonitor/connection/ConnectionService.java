package com.tsa.mqmonitor.connection;

import com.tsa.mqmonitor.config.MqMonitorProperties;
import com.tsa.mqmonitor.crypto.SecretCipher;
import com.tsa.mqmonitor.health.Thresholds;
import com.tsa.mqmonitor.mq.MqGateway;
import com.tsa.mqmonitor.mq.MqGatewayException;
import com.tsa.mqmonitor.mq.MqGatewayFactory;
import com.tsa.mqmonitor.mq.MqTarget;
import com.tsa.mqmonitor.mq.TlsSettings;
import com.tsa.mqmonitor.sampling.SamplerRegistry;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.dao.DataAccessException;
import org.springframework.data.relational.core.conversion.DbActionExecutionException;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

@Service
public class ConnectionService {
  private final MqConnectionRepository repo;
  private final SecretCipher cipher;
  private final MqMonitorProperties props;
  private final MqGatewayFactory gatewayFactory;
  private final ObjectProvider<SamplerRegistry> registry;

  public ConnectionService(
      MqConnectionRepository repo,
      SecretCipher cipher,
      MqMonitorProperties props,
      MqGatewayFactory gatewayFactory,
      ObjectProvider<SamplerRegistry> registry) {
    this.repo = repo;
    this.cipher = cipher;
    this.props = props;
    this.gatewayFactory = gatewayFactory;
    this.registry = registry;
  }

  public List<MqConnection> findAll() {
    return repo.findAll();
  }

  public Optional<MqConnection> findById(long id) {
    return repo.findById(id);
  }

  public List<MqConnection> findPrometheusExports() {
    return repo.findByPrometheusExportTrue();
  }

  public void delete(long id) {
    repo.deleteById(id);
    registry.ifAvailable(r -> r.connectionDeleted(id));
  }

  public MqConnection save(ConnectionForm form) {
    Optional<MqConnection> existing = existingFor(form);
    String passwordEncrypted =
        StringUtils.hasText(form.getPassword())
            ? cipher.encrypt(form.getPassword())
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
            form.getQueueFilter(),
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
      saved = repo.save(entity);
    } catch (DbActionExecutionException e) {
      if (e.getCause() instanceof DataAccessException dae) {
        throw dae;
      }
      throw e;
    }
    registry.ifAvailable(r -> r.connectionChanged(saved.id()));
    return saved;
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
    return cipher.decrypt(c.passwordEncrypted());
  }

  public Thresholds thresholdsFor(MqConnection c) {
    var t = props.thresholds();
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
      gateway = gatewayFactory.open(targetFor(form));
      var qmgr = gateway.inquireQueueManager();
      return new ConnectionTestResult(true, "Connected to " + qmgr.name(), qmgr);
    } catch (MqGatewayException e) {
      return new ConnectionTestResult(false, e.getMessage(), null);
    } finally {
      if (gateway != null) {
        gateway.close();
      }
    }
  }

  private Optional<MqConnection> existingFor(ConnectionForm form) {
    return form.getId() == null ? Optional.empty() : repo.findById(form.getId());
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
        Duration.ofSeconds(props.mq().connectTimeoutSeconds()));
  }

  private TlsSettings tlsFor(
      TlsMode mode, String cipherSpec, String truststore, String keystore, String label) {
    if (mode == null || mode == TlsMode.NONE) {
      return TlsSettings.none();
    }
    var d = props.tls();
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
