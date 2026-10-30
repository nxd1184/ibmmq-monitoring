package com.tsa.mqmonitor.metrics;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

import com.tsa.mqmonitor.connection.ClientType;
import com.tsa.mqmonitor.connection.ConnectionForm;
import com.tsa.mqmonitor.connection.ConnectionService;
import com.tsa.mqmonitor.connection.MqConnectionRepository;
import com.tsa.mqmonitor.connection.TlsMode;
import com.tsa.mqmonitor.health.HealthVerdict;
import com.tsa.mqmonitor.mq.MqGatewayFactory;
import com.tsa.mqmonitor.mq.QueueInfo;
import com.tsa.mqmonitor.mq.QueueManagerInfo;
import com.tsa.mqmonitor.sampling.EvaluatedSample;
import com.tsa.mqmonitor.sampling.Sample;
import com.tsa.mqmonitor.support.PostgresTestBase;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import com.tsa.mqmonitor.mq.MqGateway;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

class PrometheusEndpointTest extends PostgresTestBase {
  @Autowired MockMvc mvc;
  @Autowired ConnectionService service;
  @Autowired MqConnectionRepository repo;
  @Autowired MqMeterBinder binder;
  @MockitoBean MqGatewayFactory gatewayFactory;

  @AfterEach
  void clean() {
    repo.deleteAll();
  }

  @BeforeEach
  void gateway() {
    // The export sampler starts when the connection is saved; let it see the same queue.
    var gw = mock(MqGateway.class);
    when(gw.inquireQueueManager()).thenReturn(new QueueManagerInfo("QM1", "RUNNING", 940, "UNIX"));
    when(gw.inquireQueues(any(), anyBoolean()))
        .thenReturn(List.of(new QueueInfo("APP.Q", 7, 100, 1, 1, null, null, null)));
    when(gw.inquireChannels()).thenReturn(List.of());
    when(gatewayFactory.open(any())).thenReturn(gw);
  }

  @Test
  void exposesOnlyIbmmqSeries() throws Exception {
    var form = new ConnectionForm();
    form.setName("QM1");
    form.setHost("mq");
    form.setQueueManager("QM1");
    form.setChannel("DEV.ADMIN.SVRCONN");
    form.setUsername("admin");
    form.setPassword("pw");
    form.setClientType(ClientType.NATIVE);
    form.setTlsMode(TlsMode.NONE);
    form.setPrometheusExport(true);
    var saved = service.save(form);
    var sample =
        new Sample(saved.id(), Instant.now(), new QueueManagerInfo("QM1", "RUNNING", 940, "UNIX"),
            List.of(new QueueInfo("APP.Q", 7, 100, 1, 1, null, null, null)), List.of(), null);
    binder.onSample(new EvaluatedSample(sample, HealthVerdict.healthy(), Map.of(), Map.of()));

    var body = mvc.perform(get("/actuator/prometheus")).andReturn().getResponse().getContentAsString();

    assertThat(body).contains("ibmmq_queue_depth{").contains("connection=\"QM1\"");
    assertThat(body)
        .doesNotContain("jvm_", "process_", "system_", "http_server_", "tomcat_", "logback_",
            "executor_", "jdbc_", "hikaricp_", "disk_", "application_");
  }
}
