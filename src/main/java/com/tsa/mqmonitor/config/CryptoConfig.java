package com.tsa.mqmonitor.config;

import com.tsa.mqmonitor.security.SecretCipher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class CryptoConfig {

    @Bean
    public SecretCipher secretCipher(MqMonitorProperties props) {
        return SecretCipher.fromBase64(props.encryptionKey());
    }
}
