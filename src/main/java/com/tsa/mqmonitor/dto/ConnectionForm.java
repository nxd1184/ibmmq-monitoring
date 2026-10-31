package com.tsa.mqmonitor.dto;

import com.tsa.mqmonitor.model.ClientType;
import com.tsa.mqmonitor.model.TlsMode;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;

/** Size limits match the columns in {@code V1__connections.sql} (and MQ's own name limits). */
public class ConnectionForm {
  private Long id;
  @NotBlank
  @Size(max = 100)
  private String name;

  @NotBlank
  @Size(max = 255)
  private String host;

  @Min(1) @Max(65535)
  private int port = 1414;

  @NotBlank
  @Size(max = 48)
  private String queueManager;

  @NotBlank
  @Size(max = 20)
  private String channel;

  @Size(max = 64)
  private String username;
  private String password;
  @NotNull
  private ClientType clientType = ClientType.NATIVE;

  @NotNull
  private TlsMode tlsMode = TlsMode.NONE;

  @Size(max = 64)
  private String cipherSpec;

  @Size(max = 512)
  private String truststorePath;

  @Size(max = 512)
  private String keystorePath;

  @Size(max = 64)
  private String certificateLabel;

  @Size(max = 48)
  private String queueFilter = "*";
  private boolean hideSystemQueues = true;
  @Min(1)
  private int sampleIntervalSeconds = 5;

  private boolean prometheusExport = false;
  @Size(max = 48)
  private String replyQueueModel = "SYSTEM.DEFAULT.MODEL.QUEUE";
  private Integer thresholdDepthMax;
  private Integer thresholdDepthPercent;
  private Integer thresholdNoConsumerSeconds;
  private Integer thresholdOldestMessageAgeSeconds;

  public Long getId() {
    return id;
  }

  public void setId(Long id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public String getHost() {
    return host;
  }

  public void setHost(String host) {
    this.host = host;
  }

  public int getPort() {
    return port;
  }

  public void setPort(int port) {
    this.port = port;
  }

  public String getQueueManager() {
    return queueManager;
  }

  public void setQueueManager(String queueManager) {
    this.queueManager = queueManager;
  }

  public String getChannel() {
    return channel;
  }

  public void setChannel(String channel) {
    this.channel = channel;
  }

  public String getUsername() {
    return username;
  }

  public void setUsername(String username) {
    this.username = username;
  }

  public String getPassword() {
    return password;
  }

  public void setPassword(String password) {
    this.password = password;
  }

  public ClientType getClientType() {
    return clientType;
  }

  public void setClientType(ClientType clientType) {
    this.clientType = clientType;
  }

  public TlsMode getTlsMode() {
    return tlsMode;
  }

  public void setTlsMode(TlsMode tlsMode) {
    this.tlsMode = tlsMode;
  }

  public String getCipherSpec() {
    return cipherSpec;
  }

  public void setCipherSpec(String cipherSpec) {
    this.cipherSpec = cipherSpec;
  }

  public String getTruststorePath() {
    return truststorePath;
  }

  public void setTruststorePath(String truststorePath) {
    this.truststorePath = truststorePath;
  }

  public String getKeystorePath() {
    return keystorePath;
  }

  public void setKeystorePath(String keystorePath) {
    this.keystorePath = keystorePath;
  }

  public String getCertificateLabel() {
    return certificateLabel;
  }

  public void setCertificateLabel(String certificateLabel) {
    this.certificateLabel = certificateLabel;
  }

  public String getQueueFilter() {
    return queueFilter;
  }

  public void setQueueFilter(String queueFilter) {
    this.queueFilter = queueFilter;
  }

  public boolean isHideSystemQueues() {
    return hideSystemQueues;
  }

  public void setHideSystemQueues(boolean hideSystemQueues) {
    this.hideSystemQueues = hideSystemQueues;
  }

  public int getSampleIntervalSeconds() {
    return sampleIntervalSeconds;
  }

  public void setSampleIntervalSeconds(int sampleIntervalSeconds) {
    this.sampleIntervalSeconds = sampleIntervalSeconds;
  }

  public boolean isPrometheusExport() {
    return prometheusExport;
  }

  public void setPrometheusExport(boolean prometheusExport) {
    this.prometheusExport = prometheusExport;
  }

  public String getReplyQueueModel() {
    return replyQueueModel;
  }

  public void setReplyQueueModel(String replyQueueModel) {
    this.replyQueueModel = replyQueueModel;
  }

  public Integer getThresholdDepthMax() {
    return thresholdDepthMax;
  }

  public void setThresholdDepthMax(Integer thresholdDepthMax) {
    this.thresholdDepthMax = thresholdDepthMax;
  }

  public Integer getThresholdDepthPercent() {
    return thresholdDepthPercent;
  }

  public void setThresholdDepthPercent(Integer thresholdDepthPercent) {
    this.thresholdDepthPercent = thresholdDepthPercent;
  }

  public Integer getThresholdNoConsumerSeconds() {
    return thresholdNoConsumerSeconds;
  }

  public void setThresholdNoConsumerSeconds(Integer thresholdNoConsumerSeconds) {
    this.thresholdNoConsumerSeconds = thresholdNoConsumerSeconds;
  }

  public Integer getThresholdOldestMessageAgeSeconds() {
    return thresholdOldestMessageAgeSeconds;
  }

  public void setThresholdOldestMessageAgeSeconds(Integer thresholdOldestMessageAgeSeconds) {
    this.thresholdOldestMessageAgeSeconds = thresholdOldestMessageAgeSeconds;
  }
}
