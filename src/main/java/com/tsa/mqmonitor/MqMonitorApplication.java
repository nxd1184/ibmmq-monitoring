package com.tsa.mqmonitor;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

@SpringBootApplication
@ConfigurationPropertiesScan
public class MqMonitorApplication {
    static {
        // Lets the MQ Java and JMS clients accept JSSE cipher suite names (and the ANY_TLS* aliases)
        // on any JDK; set before any MQ client class reads it.
        System.setProperty("com.ibm.mq.cfg.useIBMCipherMappings", "false");
    }

    public static void main(String[] args) {
        SpringApplication.run(MqMonitorApplication.class, args);
    }
}
