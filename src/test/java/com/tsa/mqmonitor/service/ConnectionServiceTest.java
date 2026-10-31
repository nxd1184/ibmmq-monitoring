package com.tsa.mqmonitor.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.timeout;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.tsa.mqmonitor.client.MqGateway;
import com.tsa.mqmonitor.client.MqGatewayFactory;
import com.tsa.mqmonitor.exception.MqUnreachableException;
import com.tsa.mqmonitor.model.ClientType;
import com.tsa.mqmonitor.model.ConnectionForm;
import com.tsa.mqmonitor.model.QueueManagerInfo;
import com.tsa.mqmonitor.model.Thresholds;
import com.tsa.mqmonitor.model.TlsMode;
import com.tsa.mqmonitor.repository.MqConnectionRepository;
import com.tsa.mqmonitor.security.SecretCipher;
import com.tsa.mqmonitor.support.PostgresTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

class ConnectionServiceTest extends PostgresTestBase {
  @Autowired ConnectionService service;
  @Autowired MqConnectionRepository repo;
  @Autowired SecretCipher cipher;
  @MockitoBean MqGatewayFactory gatewayFactory;

  @BeforeEach
  void clean() {
    repo.deleteAll();
  }

  ConnectionForm valid() {
    var f = new ConnectionForm();
    f.setName("QM1");
    f.setHost("mq");
    f.setPort(1414);
    f.setQueueManager("QM1");
    f.setChannel("DEV.ADMIN.SVRCONN");
    f.setUsername("admin");
    f.setPassword("pw");
    f.setClientType(ClientType.NATIVE);
    f.setTlsMode(TlsMode.NONE);
    f.setQueueFilter("*");
    f.setSampleIntervalSeconds(5);
    f.setHideSystemQueues(true);
    f.setReplyQueueModel("SYSTEM.DEFAULT.MODEL.QUEUE");
    return f;
  }

  @Test
  void savesEncryptedPassword() {
    var saved = service.save(valid());
    assertThat(saved.passwordEncrypted()).isNotEqualTo("pw");
    assertThat(cipher.decrypt(saved.passwordEncrypted())).isEqualTo("pw");
  }

  @Test
  void blankPasswordOnEditKeepsExisting() {
    var saved = service.save(valid());
    var edit = service.toForm(saved);
    assertThat(edit.getPassword()).isNull();
    edit.setPassword("");
    edit.setHost("mq2");
    var again = service.save(edit);
    assertThat(again.host()).isEqualTo("mq2");
    assertThat(service.plainPassword(again)).isEqualTo("pw");
  }

  @Test
  void thresholdsFallBackToGlobalDefaults() {
    var saved = service.save(valid());
    assertThat(service.thresholdsFor(saved)).isEqualTo(new Thresholds(null, 80, 60, null));
  }

  @Test
  void thresholdOverrideWins() {
    var f = valid();
    f.setThresholdDepthPercent(50);
    assertThat(service.thresholdsFor(service.save(f)).depthPercent()).isEqualTo(50);
  }

  @Test
  void testConnectionReportsQmgr() throws Exception {
    var gw = mock(MqGateway.class);
    when(gw.inquireQueueManager()).thenReturn(new QueueManagerInfo("QM1", "RUNNING", 940, "UNIX"));
    when(gatewayFactory.open(any())).thenReturn(gw);
    var r = service.test(valid());
    assertThat(r.ok()).isTrue();
    assertThat(r.qmgr().name()).isEqualTo("QM1");
    verify(gw, timeout(2000)).close();
  }

  @Test
  void testConnectionReportsFailure() {
    when(gatewayFactory.open(any()))
        .thenThrow(new MqUnreachableException("MQRC 2538 host not available", null));
    var r = service.test(valid());
    assertThat(r.ok()).isFalse();
    assertThat(r.message()).contains("2538");
  }

  @Test
  void testConnectionReportsUnexpectedFailureInsteadOfThrowing() {
    when(gatewayFactory.open(any())).thenThrow(new IllegalStateException("truststore unreadable"));
    var r = service.test(valid());
    assertThat(r.ok()).isFalse();
    assertThat(r.message()).isEqualTo("truststore unreadable");
  }

  @Test
  void blankQueueFilterIsStoredAsWildcard() {
    var f = valid();
    f.setQueueFilter("  ");
    assertThat(service.save(f).queueFilter()).isEqualTo("*");
  }

  @Test
  void duplicateNameRejected() {
    service.save(valid());
    assertThatThrownBy(() -> service.save(valid())).isInstanceOf(DuplicateKeyException.class);
  }
}
