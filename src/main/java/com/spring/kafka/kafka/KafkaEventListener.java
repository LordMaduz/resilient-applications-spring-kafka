package com.spring.kafka.kafka;

import com.spring.kafka.model.Order;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.TopicSuffixingStrategy;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaEventListener {

    @KafkaListener(id = "ORDER_PROCESSED_GROUP", topics = "order-event",
            containerFactory = "kafkaListenerContainerFactory")
    public void onOrderEventReceived(ConsumerRecord<String, Order> record,
                                     Acknowledgment acknowledgment) {
        Order order = record.value();
        log.info("Order Processed Event Received: {}", order);
        if(order.getCount() % 5 == 0 ){
            throw new RuntimeException("Error");
        }
        log.info("Order Processed Event Consumed: {}", order);
        acknowledgment.acknowledge();
    }

    @RetryableTopic(
            attempts = "3",
            topicSuffixingStrategy = TopicSuffixingStrategy.SUFFIX_WITH_INDEX_VALUE,
            autoCreateTopics = "false",
            backoff = @Backoff(delay = 1000, maxDelay = 3000, random = true)
    )
    @KafkaListener(id = "ORDER_PROCESSED_RETRYABLE_GROUP", topics = "order-event",
            containerFactory = "kafkaListenerContainerFactoryWithRetry")
    public void onOrderEventReceivedWithRetryAnnotation(ConsumerRecord<String, Order> record,
                                     Acknowledgment acknowledgment) {
        Order order = record.value();
        log.info("Order Processed Event Received: {}", order);
        if(order.getCount() % 5 == 0 ){
            throw new RuntimeException("Error");
        }
        log.info("Order Processed Event Consumed: {}", order);
        acknowledgment.acknowledge();
    }
}
