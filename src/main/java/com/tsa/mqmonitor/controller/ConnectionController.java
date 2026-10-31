package com.tsa.mqmonitor.controller;

import com.tsa.mqmonitor.config.MqMonitorProperties;
import com.tsa.mqmonitor.enumerable.ClientType;
import com.tsa.mqmonitor.enumerable.TlsMode;
import com.tsa.mqmonitor.model.ConnectionForm;
import com.tsa.mqmonitor.model.ConnectionTestResult;
import com.tsa.mqmonitor.model.MqConnection;
import com.tsa.mqmonitor.service.ConnectionService;
import com.tsa.mqmonitor.service.SamplerRegistry;
import com.tsa.mqmonitor.validation.ConnectionFormValidator;
import jakarta.validation.Valid;
import java.util.List;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.validation.BindingResult;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.server.ResponseStatusException;

@Controller
class ConnectionController {
  private static final String FORM = "connections/form";

  private final ConnectionService connectionService;
  private final ConnectionFormValidator connectionFormValidator;
  private final SamplerRegistry samplerRegistry;
  private final SampleFragmentRenderer sampleFragmentRenderer;
  private final MqMonitorProperties mqMonitorProperties;

  ConnectionController(
      ConnectionService connectionService,
      ConnectionFormValidator connectionFormValidator,
      SamplerRegistry samplerRegistry,
      SampleFragmentRenderer sampleFragmentRenderer,
      MqMonitorProperties mqMonitorProperties) {
    this.connectionService = connectionService;
    this.connectionFormValidator = connectionFormValidator;
    this.samplerRegistry = samplerRegistry;
    this.sampleFragmentRenderer = sampleFragmentRenderer;
    this.mqMonitorProperties = mqMonitorProperties;
  }

  @InitBinder("connectionForm")
  void initBinder(WebDataBinder binder) {
    binder.addValidators(connectionFormValidator);
  }

  @ModelAttribute("clientTypes")
  ClientType[] clientTypes() {
    return ClientType.values();
  }

  @ModelAttribute("tlsModes")
  TlsMode[] tlsModes() {
    return TlsMode.values();
  }

  @ModelAttribute("pageTitle")
  String pageTitle(@PathVariable(required = false) Long id) {
    return id == null ? "New connection" : "Edit connection";
  }

  @ModelAttribute("thresholdDefaults")
  MqMonitorProperties.Thresholds thresholdDefaults() {
    return mqMonitorProperties.thresholds();
  }

  @GetMapping("/")
  String root() {
    return "redirect:/connections";
  }

  @GetMapping("/connections")
  String list(Model model) {
    List<String> rows =
        connectionService.findAll().stream()
            .map(c -> sampleFragmentRenderer.renderRow(c, samplerRegistry.latest(c.id()).orElse(null)))
            .toList();
    model.addAttribute("rows", rows);
    return "connections/list";
  }

  @GetMapping("/connections/new")
  String newForm(Model model) {
    model.addAttribute("connectionForm", new ConnectionForm());
    return FORM;
  }

  @PostMapping("/connections")
  String create(
      @Valid @ModelAttribute("connectionForm") ConnectionForm form, BindingResult result) {
    form.setId(null);
    return persist(form, result);
  }

  @GetMapping("/connections/{id}/edit")
  String edit(@PathVariable long id, Model model) {
    model.addAttribute("connectionForm", connectionService.toForm(find(id)));
    return FORM;
  }

  @PostMapping("/connections/{id}")
  String update(
      @PathVariable long id,
      @Valid @ModelAttribute("connectionForm") ConnectionForm form,
      BindingResult result) {
    find(id);
    form.setId(id);
    return persist(form, result);
  }

  @PostMapping("/connections/{id}/delete")
  Object delete(
      @PathVariable long id,
      @RequestHeader(name = "HX-Request", required = false) String htmx) {
    connectionService.delete(id);
    if (htmx != null) {
      return ResponseEntity.ok().body("");
    }
    return "redirect:/connections";
  }

  @PostMapping("/connections/test")
  String test(
      @Valid @ModelAttribute("connectionForm") ConnectionForm form,
      BindingResult result,
      Model model) {
    if (result.hasErrors()) {
      model.addAttribute("errors", messages(result));
      model.addAttribute("result", null);
    } else {
      ConnectionTestResult outcome = connectionService.test(form);
      model.addAttribute("errors", List.of());
      model.addAttribute("result", outcome);
    }
    return "fragments/test-result :: content";
  }

  @GetMapping("/connections/tls-fields")
  String tlsFields(@ModelAttribute("connectionForm") ConnectionForm form) {
    return "fragments/tls-fields :: fields";
  }

  private String persist(ConnectionForm form, BindingResult result) {
    if (!result.hasErrors()) {
      try {
        connectionService.save(form);
        return "redirect:/connections";
      } catch (DuplicateKeyException e) {
        result.rejectValue(
            "name", "name.duplicate", "a connection with this name already exists");
      }
    }
    return FORM;
  }

  private MqConnection find(long id) {
    return connectionService
        .findById(id)
        .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND));
  }

  private static List<String> messages(BindingResult result) {
    return result.getAllErrors().stream()
        .map(
            e ->
                e instanceof FieldError fe
                    ? fe.getField() + ": " + fe.getDefaultMessage()
                    : e.getDefaultMessage())
        .toList();
  }
}
