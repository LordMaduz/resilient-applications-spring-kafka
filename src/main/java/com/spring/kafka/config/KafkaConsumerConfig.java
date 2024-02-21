package com.spring.kafka.config;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;

@Slf4j
@EnableKafka
@Configuration
@RequiredArgsConstructor
public class KafkaConsumerConfig extends KafkaBasicConfig {

    private final KafkaTemplate<?,?> kafkaTemplate;

    @Bean
    public ConsumerFactory<String, Object> consumerFactory() {
        final Map<String, Object> basicConfig = getBasicConfig();

        basicConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        basicConfig.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        return new DefaultKafkaConsumerFactory<>(basicConfig, new StringDeserializer(), jsonDeserializer());

    }

    @Bean()
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory() {
        final ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConcurrency(6);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setConsumerFactory(consumerFactory());
        factory.setCommonErrorHandler(errorHandler());
        //factory.setCommonErrorHandler(deadLetterRecoveryErrorHandler());
        return factory;
    }

    @Bean()
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactoryWithRetryAnnotation() {
        final ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConcurrency(6);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL_IMMEDIATE);
        factory.setConsumerFactory(consumerFactory());
        return factory;
    }


    private <T> JsonDeserializer<T> jsonDeserializer(){
        final JsonDeserializer<T> jsonDeserializer = new JsonDeserializer<>();
        jsonDeserializer.addTrustedPackages("*");
        jsonDeserializer.setRemoveTypeHeaders(false);

        return jsonDeserializer;
    }

    private DefaultErrorHandler errorHandler() {
        return new DefaultErrorHandler((record, exception) -> {

            log.error("!!!!!!!!!!! Following Record keeps failing despite multiple attempts, therefore would be skipped !! : " + record);
            log.error(exception.getMessage());
        }, new FixedBackOff(1000L, 2L));

    }

    private DefaultErrorHandler deadLetterRecoveryErrorHandler() {
        final DeadLetterPublishingRecoverer deadLetterPublishingRecovery = new DeadLetterPublishingRecoverer(kafkaTemplate,
                (consumerRecord, exception) -> {
                    log.error("!!!!!!!!!!! Following Record keeps failing despite multiple attempts, therefore would be skipped !! : " + consumerRecord);
                    return new TopicPartition("order-event-dlt", consumerRecord.partition());
                }
        );
        return new DefaultErrorHandler(deadLetterPublishingRecovery, new FixedBackOff(1000L, 2L));

    }
}
