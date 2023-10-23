package com.lazroproj.reactivekafka;

import com.liderbet.sportradar.RawMatch;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.Collections;
import java.util.HashMap;

@Configuration
@Slf4j
public class KafkaConsumerConfig {

    @Bean
    public ReceiverOptions<String, RawMatch> kafkaReceiverOptions(KafkaProperties kafkaProperties){
        HashMap<String, Object> props = new HashMap<>(kafkaProperties.buildConsumerProperties());
        ReceiverOptions<String, RawMatch> basicReceiverOptions = ReceiverOptions.create(props);
        return basicReceiverOptions.subscription(Collections.singletonList(kafkaProperties.getTemplate().getDefaultTopic()));
    }

    @Bean
    public ReactiveKafkaConsumerTemplate<String, RawMatch> reactiveKafkaConsumerTemplate(ReceiverOptions<String, RawMatch> kafkaReceiverOptions) {
        return new ReactiveKafkaConsumerTemplate<>(kafkaReceiverOptions);
    }
}

