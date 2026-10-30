package com.tsa.mqmonitor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class MqMonitorApplication {

    public static void main(String[] args) {
        SpringApplication.run(MqMonitorApplication.class, args);
    }
}
