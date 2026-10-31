package com.tsa.mqmonitor.controller;

import com.tsa.mqmonitor.model.EvaluatedSample;
import com.tsa.mqmonitor.model.MqConnection;
import com.tsa.mqmonitor.service.ConnectionService;
import com.tsa.mqmonitor.service.SamplerRegistry;
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
  private final ConnectionService connections;
  private final SamplerRegistry registry;
  private final SampleFragmentRenderer renderer;

  DashboardController(
      ConnectionService connections, SamplerRegistry registry, SampleFragmentRenderer renderer) {
    this.connections = connections;
    this.registry = registry;
    this.renderer = renderer;
  }

  @GetMapping("/connections/{id}")
  String dashboard(
      @PathVariable long id,
      @RequestParam(defaultValue = "false") boolean showSystem,
      Model model) {
    MqConnection connection =
        connections
            .findById(id)
            .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
    EvaluatedSample latest = registry.latest(id).orElse(null);
    Map<String, String> fragments = new LinkedHashMap<>();
    if (latest != null) {
      // A suspended sampler keeps its last sample; never label an old one "Live".
      boolean stale = registry.isStale(id, latest);
      for (String name : new String[] {"qmgr", "queues", "channels"}) {
        fragments.put(name, renderer.render(name, latest, showSystem, stale));
      }
    }
    model.addAttribute("connection", connection);
    model.addAttribute("latest", latest);
    model.addAttribute("showSystem", showSystem);
    model.addAttribute("fragments", fragments);
    return "dashboard/index";
  }
}
