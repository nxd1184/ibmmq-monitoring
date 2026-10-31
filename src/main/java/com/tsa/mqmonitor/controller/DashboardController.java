package com.tsa.mqmonitor.controller;

import com.tsa.mqmonitor.model.EvaluatedSample;
import com.tsa.mqmonitor.model.MqConnection;
import com.tsa.mqmonitor.service.impl.ConnectionService;
import com.tsa.mqmonitor.service.impl.SamplerRegistry;
import java.util.LinkedHashMap;
import java.util.Map;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.server.ResponseStatusException;

@Controller
class DashboardController {
  private final ConnectionService connectionService;
  private final SamplerRegistry samplerRegistry;
  private final SampleFragmentRenderer sampleFragmentRenderer;

  DashboardController(
      ConnectionService connectionService,
      SamplerRegistry samplerRegistry,
      SampleFragmentRenderer sampleFragmentRenderer) {
    this.connectionService = connectionService;
    this.samplerRegistry = samplerRegistry;
    this.sampleFragmentRenderer = sampleFragmentRenderer;
  }

  @GetMapping("/connections/{id}")
  String dashboard(
      @PathVariable long id,
      @RequestParam(defaultValue = "false") boolean showSystem,
      Model model) {
    MqConnection connection =
        connectionService
            .findById(id)
            .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
    EvaluatedSample latest = samplerRegistry.latest(id).orElse(null);
    Map<String, String> fragments = new LinkedHashMap<>();
    if (latest != null) {
      // A suspended sampler keeps its last sample; never label an old one "Live".
      boolean stale = samplerRegistry.isStale(id, latest);
      for (String name : new String[] {"qmgr", "queues", "channels"}) {
        fragments.put(name, sampleFragmentRenderer.render(name, latest, showSystem, stale));
      }
    }
    model.addAttribute("connection", connection);
    model.addAttribute("latest", latest);
    model.addAttribute("showSystem", showSystem);
    model.addAttribute("fragments", fragments);
    return "dashboard/index";
  }
}
