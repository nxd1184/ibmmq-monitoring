package com.tsa.mqmonitor.controller;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.user;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.redirectedUrlPattern;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.tsa.mqmonitor.enumerable.ClientType;
import com.tsa.mqmonitor.enumerable.TlsMode;
import com.tsa.mqmonitor.model.ConnectionForm;
import com.tsa.mqmonitor.repository.MqConnectionRepository;
import com.tsa.mqmonitor.service.ConnectionService;
import com.tsa.mqmonitor.service.SamplerRegistry;
import com.tsa.mqmonitor.support.PostgresTestBase;
import java.util.Optional;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

class DashboardControllerTest extends PostgresTestBase {
  @Autowired MockMvc mockMvc;
  @Autowired ConnectionService connections;
  @Autowired MqConnectionRepository repo;
  @MockitoBean SamplerRegistry registry;

  long id;

  @BeforeEach
  void saveConnection() {
    repo.deleteAll();
    var f = new ConnectionForm();
    f.setName("Payments QM");
    f.setHost("mq");
    f.setPort(1414);
    f.setQueueManager("QM1");
    f.setChannel("DEV.ADMIN.SVRCONN");
    f.setClientType(ClientType.NATIVE);
    f.setTlsMode(TlsMode.NONE);
    id = connections.save(f).id();
    when(registry.latest(anyLong())).thenReturn(Optional.empty());
  }

  @AfterEach
  void deleteConnection() {
    repo.deleteAll();
  }

  @Test
  void pageRendersNameAndStreamUrl() throws Exception {
    mockMvc.perform(get("/connections/" + id).with(user("admin")))
        .andExpect(status().isOk())
        .andExpect(content().string(containsString("Payments QM")))
        .andExpect(content().string(containsString("sse-connect=\"/connections/" + id + "/stream\"")))
        .andExpect(content().string(containsString("sse-swap=\"qmgr\"")))
        .andExpect(content().string(containsString("sse-swap=\"queues\"")))
        .andExpect(content().string(containsString("sse-swap=\"channels\"")))
        .andExpect(content().string(containsString("Waiting for first sample")))
        .andExpect(content().string(containsString("showSystem=true")));
  }

  @Test
  void streamContainerTracksFeedDisconnects() throws Exception {
    mockMvc.perform(get("/connections/" + id).with(user("admin")))
        .andExpect(status().isOk())
        .andExpect(
            content()
                .string(containsString("hx-on::sse-error=\"this.classList.add('sse-down')\"")))
        .andExpect(content().string(containsString("hx-on::sse-open=")));
  }

  @Test
  void showSystemFlagFlowsToStreamAndToggle() throws Exception {
    mockMvc.perform(get("/connections/" + id).param("showSystem", "true").with(user("admin")))
        .andExpect(status().isOk())
        .andExpect(
            content()
                .string(
                    containsString(
                        "sse-connect=\"/connections/" + id + "/stream?showSystem=true\"")))
        .andExpect(content().string(containsString("showSystem=false")));
  }

  @Test
  void unknownIdIs404() throws Exception {
    mockMvc.perform(get("/connections/" + (id + 1000)).with(user("admin")))
        .andExpect(status().isNotFound());
  }

  @Test
  void latestSampleIsRenderedIntoThePage() throws Exception {
    when(registry.latest(id)).thenReturn(Optional.of(SseControllerTest.sample(id)));
    mockMvc.perform(get("/connections/" + id).with(user("admin")))
        .andExpect(status().isOk())
        .andExpect(content().string(containsString(">12345<")))
        .andExpect(content().string(not(containsString("Waiting for first sample"))));
  }

  @Test
  void oldRetainedSampleRendersStaleOnFirstPaint() throws Exception {
    var latest = SseControllerTest.sample(id);
    when(registry.latest(id)).thenReturn(Optional.of(latest));
    when(registry.isStale(id, latest)).thenReturn(true);
    mockMvc.perform(get("/connections/" + id).with(user("admin")))
        .andExpect(status().isOk())
        .andExpect(content().string(containsString("Stale · last sample")))
        .andExpect(content().string(not(containsString("live-word"))));
  }

  @Test
  void streamContainerReloadsOnExpiredSession() throws Exception {
    mockMvc.perform(get("/connections/" + id).with(user("admin")))
        .andExpect(content().string(containsString("data-reload-on-401")))
        .andExpect(content().string(containsString("/js/sse-auth.js")));
  }

  @Test
  void anonymousIsRedirectedToLogin() throws Exception {
    mockMvc.perform(get("/connections/" + id))
        .andExpect(status().is3xxRedirection())
        .andExpect(redirectedUrlPattern("**/login"));
  }
}
