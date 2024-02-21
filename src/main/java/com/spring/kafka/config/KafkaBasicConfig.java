package com.spring.kafka.config;

import org.springframework.context.annotation.Configuration;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaBasicConfig {

    protected Map<String, Object> getBasicConfig() {
        final Map<String, Object> config = new HashMap<>();
        config.put("bootstrap.servers", "localhost:9092");
        return config;
    }

}
