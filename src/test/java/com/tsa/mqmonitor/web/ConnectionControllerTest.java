package com.tsa.mqmonitor.web;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.emptyString;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrl;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrlPattern;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.tsa.mqmonitor.connection.ClientType;
import com.tsa.mqmonitor.connection.ConnectionForm;
import com.tsa.mqmonitor.connection.ConnectionService;
import com.tsa.mqmonitor.connection.MqConnectionRepository;
import com.tsa.mqmonitor.connection.TlsMode;
import com.tsa.mqmonitor.health.HealthReason;
import com.tsa.mqmonitor.health.HealthVerdict;
import com.tsa.mqmonitor.mq.MqGateway;
import com.tsa.mqmonitor.mq.MqGatewayFactory;
import com.tsa.mqmonitor.mq.MqUnreachableException;
import com.tsa.mqmonitor.mq.QueueManagerInfo;
import com.tsa.mqmonitor.sampling.EvaluatedSample;
import com.tsa.mqmonitor.sampling.SamplerRegistry;
import com.tsa.mqmonitor.support.PostgresTestBase;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;

class ConnectionControllerTest extends PostgresTestBase {
  @Autowired MockMvc mvc;
  @Autowired ConnectionService connections;
  @Autowired MqConnectionRepository repo;
  @MockitoBean MqGatewayFactory factory;
  @MockitoBean SamplerRegistry registry;

  @BeforeEach
  void clean() {
    repo.deleteAll();
    when(registry.latest(anyLong())).thenReturn(Optional.empty());
  }

  ConnectionForm valid(String name) {
    var f = new ConnectionForm();
    f.setName(name);
    f.setHost("mq");
    f.setPort(1414);
    f.setQueueManager("QM1");
    f.setChannel("DEV.ADMIN.SVRCONN");
    f.setPassword("s3cret-pw");
    return f;
  }

  MockHttpServletRequestBuilder validPost(String url, String name, String... overrides) {
    Map<String, String> p = new java.util.LinkedHashMap<>();
    p.put("name", name);
    p.put("host", "mq");
    p.put("port", "1414");
    p.put("queueManager", "QM1");
    p.put("channel", "DEV.ADMIN.SVRCONN");
    p.put("clientType", "NATIVE");
    p.put("tlsMode", "NONE");
    p.put("sampleIntervalSeconds", "5");
    p.put("queueFilter", "*");
    p.put("replyQueueModel", "SYSTEM.DEFAULT.MODEL.QUEUE");
    for (int i = 0; i < overrides.length; i += 2) {
      p.put(overrides[i], overrides[i + 1]);
    }
    var req = post(url).with(user("admin")).with(csrf());
    p.forEach(req::param);
    return req;
  }

  @Test
  void listRequiresLogin() throws Exception {
    mvc.perform(get("/connections"))
        .andExpect(status().is3xxRedirection())
        .andExpect(redirectedUrlPattern("**/login"));
  }

  @Test
  void rootRedirectsToConnections() throws Exception {
    mvc.perform(get("/").with(user("admin")))
        .andExpect(status().is3xxRedirection())
        .andExpect(redirectedUrl("/connections"));
  }

  @Test
  void listShowsEmptyState() throws Exception {
    mvc.perform(get("/connections").with(user("admin")))
        .andExpect(status().isOk())
        .andExpect(content().string(containsString("New connection")))
        .andExpect(content().string(containsString("No connections")));
  }

  @Test
  void createPersistsAndRedirects() throws Exception {
    mvc.perform(validPost("/connections", "Prod"))
        .andExpect(status().is3xxRedirection())
        .andExpect(redirectedUrl("/connections"));
    assertThat(repo.count()).isEqualTo(1);
  }

  @Test
  void duplicateNameShowsFieldError() throws Exception {
    connections.save(valid("Prod"));
    mvc.perform(validPost("/connections", "Prod"))
        .andExpect(status().isOk())
        .andExpect(content().string(containsString("a connection with this name already exists")));
    assertThat(repo.count()).isEqualTo(1);
  }

  @Test
  void createWithTlsMissingCipherShowsError() throws Exception {
    mvc.perform(validPost("/connections", "Prod", "tlsMode", "TLS"))
        .andExpect(status().isOk())
        .andExpect(content().string(containsString("Cipher spec is required for TLS")));
    assertThat(repo.count()).isZero();
  }

  @Test
  void newFormShowsDefaults() throws Exception {
    mvc.perform(get("/connections/new").with(user("admin")))
        .andExpect(status().isOk())
        .andExpect(content().string(containsString("value=\"1414\"")))
        .andExpect(content().string(containsString("value=\"*\"")))
        .andExpect(content().string(containsString("value=\"5\"")))
        .andExpect(content().string(containsString("action=\"/connections\"")));
  }

  @Test
  void editFormHasNoPassword() throws Exception {
    long id = connections.save(valid("Prod")).id();
    mvc.perform(get("/connections/" + id + "/edit").with(user("admin")))
        .andExpect(status().isOk())
        .andExpect(content().string(containsString("name=\"password\"")))
        .andExpect(content().string(containsString("leave blank to keep the current password")))
        .andExpect(content().string(not(containsString("s3cret-pw"))))
        .andExpect(content().string(containsString("action=\"/connections/" + id + "\"")));
    // the password input carries no value at all
    String html =
        mvc.perform(get("/connections/" + id + "/edit").with(user("admin")))
            .andReturn()
            .getResponse()
            .getContentAsString();
    int i = html.indexOf("name=\"password\"");
    String tag = html.substring(html.lastIndexOf('<', i), html.indexOf('>', i));
    assertThat(tag).doesNotContain("value=\"s");
  }

  @Test
  void updateKeepsPasswordWhenBlank() throws Exception {
    long id = connections.save(valid("Prod")).id();
    String before = repo.findById(id).orElseThrow().passwordEncrypted();
    mvc.perform(validPost("/connections/" + id, "Renamed", "channel", "OTHER"))
        .andExpect(status().is3xxRedirection());
    var after = repo.findById(id).orElseThrow();
    assertThat(after.name()).isEqualTo("Renamed");
    assertThat(after.passwordEncrypted()).isEqualTo(before);
  }

  @Test
  void testEndpointDoesNotPersist() throws Exception {
    var gateway = mock(MqGateway.class);
    when(gateway.inquireQueueManager())
        .thenReturn(new QueueManagerInfo("QM1", "RUNNING", 940, "UNIX"));
    when(factory.open(any())).thenReturn(gateway);
    mvc.perform(validPost("/connections/test", "Prod"))
        .andExpect(status().isOk())
        .andExpect(content().string(containsString("QM1")));
    assertThat(repo.count()).isZero();
  }

  @Test
  void testEndpointShowsFailure() throws Exception {
    when(factory.open(any())).thenThrow(new MqUnreachableException("MQRC 2538 host not found", null));
    mvc.perform(validPost("/connections/test", "Prod"))
        .andExpect(status().isOk())
        .andExpect(content().string(containsString("2538")));
    assertThat(repo.count()).isZero();
  }

  @Test
  void testEndpointWithInvalidFormShowsErrors() throws Exception {
    mvc.perform(validPost("/connections/test", "Prod", "tlsMode", "TLS", "host", ""))
        .andExpect(status().isOk())
        .andExpect(content().string(containsString("Cipher spec is required for TLS")));
    verify(factory, org.mockito.Mockito.never()).open(any());
    assertThat(repo.count()).isZero();
  }

  @Test
  void tlsFieldsFragmentShowsKeystoreForMutualTls() throws Exception {
    mvc.perform(get("/connections/tls-fields").param("tlsMode", "MUTUAL_TLS").with(user("admin")))
        .andExpect(status().isOk())
        .andExpect(content().string(containsString("name=\"cipherSpec\"")))
        .andExpect(content().string(containsString("name=\"truststorePath\"")))
        .andExpect(content().string(containsString("name=\"keystorePath\"")))
        .andExpect(content().string(containsString("name=\"certificateLabel\"")));
    mvc.perform(get("/connections/tls-fields").param("tlsMode", "TLS").with(user("admin")))
        .andExpect(content().string(containsString("name=\"truststorePath\"")))
        .andExpect(content().string(not(containsString("name=\"keystorePath\""))));
    mvc.perform(get("/connections/tls-fields").param("tlsMode", "NONE").with(user("admin")))
        .andExpect(content().string(not(containsString("name=\"cipherSpec\""))));
  }

  @Test
  void tlsFieldsFragmentKeepsTypedValues() throws Exception {
    mvc.perform(
            get("/connections/tls-fields")
                .param("tlsMode", "MUTUAL_TLS")
                .param("cipherSpec", "TLS_AES_128_GCM_SHA256")
                .param("truststorePath", "/etc/mq/trust.p12")
                .with(user("admin")))
        .andExpect(status().isOk())
        .andExpect(content().string(containsString("value=\"TLS_AES_128_GCM_SHA256\"")))
        .andExpect(content().string(containsString("value=\"/etc/mq/trust.p12\"")))
        .andExpect(content().string(containsString("name=\"keystorePath\"")));
  }

  @Test
  void deleteRemovesRow() throws Exception {
    long id = connections.save(valid("Prod")).id();
    mvc.perform(post("/connections/" + id + "/delete").with(user("admin")).with(csrf()))
        .andExpect(status().is3xxRedirection())
        .andExpect(redirectedUrl("/connections"));
    assertThat(repo.count()).isZero();
    verify(registry).connectionDeleted(id);
  }

  @Test
  void deleteViaHtmxReturnsEmptyBody() throws Exception {
    long id = connections.save(valid("Prod")).id();
    mvc.perform(
            post("/connections/" + id + "/delete")
                .header("HX-Request", "true")
                .with(user("admin"))
                .with(csrf()))
        .andExpect(status().isOk())
        .andExpect(content().string(emptyString()));
    assertThat(repo.count()).isZero();
  }

  @Test
  void listShowsHealthFromLatestSample() throws Exception {
    long id = connections.save(valid("Prod")).id();
    var base = SseControllerTest.sample(id);
    var unhealthy =
        new EvaluatedSample(
            base.sample(),
            HealthVerdict.unhealthy(HealthReason.UNREACHABLE, "down"),
            Map.of(),
            Map.of());
    when(registry.latest(id)).thenReturn(Optional.of(unhealthy));
    mvc.perform(get("/connections").with(user("admin")))
        .andExpect(status().isOk())
        .andExpect(content().string(containsString("health-unhealthy")))
        .andExpect(content().string(containsString("sse-connect=\"/connections/stream\"")))
        .andExpect(content().string(containsString("id=\"conn-" + id + "\"")))
        .andExpect(content().string(containsString("/connections/" + id + "/edit")))
        .andExpect(content().string(containsString("sse-swap=\"conn-" + id + "\"")))
        .andExpect(content().string(containsString("hx-headers=\"{&quot;X-CSRF-TOKEN&quot;")))
        .andExpect(content().string(containsString("hx-post=\"/connections/" + id + "/delete\"")));
  }
}
