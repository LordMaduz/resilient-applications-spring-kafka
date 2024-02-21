package com.spring.kafka.handler;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class KafkaPublisherHandler {

    public void publish(final KafkaTemplate<String, Object> template, final String topic, final Object payload) {

        template.send(topic, payload).whenComplete((result, exception) -> {
            if (exception == null) {
                log.info("Event Published Successfully with Offset: {}", result.getRecordMetadata().offset());
                return;
            }
            log.info("Unable to Publish Message: {}", exception, exception);
        });
    }
}