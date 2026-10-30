package com.tsa.mqmonitor;

import com.tsa.mqmonitor.config.MqMonitorProperties;
import com.tsa.mqmonitor.support.PostgresTestBase;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;

import static org.assertj.core.api.Assertions.assertThat;

class ApplicationStartupTest extends PostgresTestBase {

    @Autowired
    MqMonitorProperties props;

    @Test
    void contextLoadsAndFlywayRan(@Autowired JdbcTemplate jdbc) {
        assertThat(jdbc.queryForObject("select count(*) from mq_connection", Integer.class)).isZero();
        assertThat(props.admin().username()).isEqualTo("admin");
    }

    @Test
    void documentedDefaultsApply() {
        assertThat(props.sampling().watcherGraceSeconds()).isEqualTo(30);
        assertThat(props.sampling().listIntervalSeconds()).isEqualTo(30);
        assertThat(props.thresholds().depthPercent()).isEqualTo(80);
        assertThat(props.thresholds().noConsumerSeconds()).isEqualTo(60);
        assertThat(props.mq().connectTimeoutSeconds()).isEqualTo(10);
    }
}
