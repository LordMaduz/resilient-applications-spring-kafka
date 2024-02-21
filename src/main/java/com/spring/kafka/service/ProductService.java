package com.spring.kafka.service;

import com.spring.kafka.handler.KafkaPublisherHandler;
import com.spring.kafka.model.Order;
import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.util.stream.IntStream;

@Service
@RequiredArgsConstructor
public class ProductService {

    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final KafkaPublisherHandler kafkaPublisherHandler;

    public void publishMessages() {

        IntStream.range(4, 7).forEach(i -> {
            final Order event = order(i);
            kafkaPublisherHandler.publish(kafkaTemplate, "order-event", event);
        });
    }

    private Order order(final Integer count) {
        final Order event = new Order();

        event.setPayload("payload");
        event.setCount(count);
        event.setCreatedDate(LocalDateTime.now());

        return event;
    }
}
