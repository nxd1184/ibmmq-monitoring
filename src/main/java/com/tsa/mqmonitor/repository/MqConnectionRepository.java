package com.tsa.mqmonitor.repository;

import com.tsa.mqmonitor.model.MqConnection;
import java.util.List;
import java.util.Optional;
import org.springframework.data.repository.ListCrudRepository;

public interface MqConnectionRepository extends ListCrudRepository<MqConnection, Long> {
  Optional<MqConnection> findByName(String name);

  List<MqConnection> findByPrometheusExportTrue();
}
