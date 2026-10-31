package com.tsa.mqmonitor;

import static org.assertj.core.api.Assertions.assertThat;

import com.tsa.mqmonitor.config.MqMonitorProperties;
import com.tsa.mqmonitor.support.PostgresTestBase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

class ApplicationStartupTest extends PostgresTestBase {

    @Autowired
    MqMonitorProperties mqMonitorProperties;

    @Test
    void contextLoadsAndFlywayRan(@Autowired JdbcTemplate jdbc) {
        assertThat(jdbc.queryForObject("select count(*) from mq_connection", Integer.class)).isZero();
        assertThat(mqMonitorProperties.admin().username()).isEqualTo("admin");
    }

    @Test
    void documentedDefaultsApply() {
        assertThat(mqMonitorProperties.sampling().watcherGraceSeconds()).isEqualTo(30);
        assertThat(mqMonitorProperties.sampling().listIntervalSeconds()).isEqualTo(30);
        assertThat(mqMonitorProperties.thresholds().depthPercent()).isEqualTo(80);
        assertThat(mqMonitorProperties.thresholds().noConsumerSeconds()).isEqualTo(60);
        assertThat(mqMonitorProperties.mq().connectTimeoutSeconds()).isEqualTo(10);
    }
}
