package com.lazroproj.reactivekafka;

import com.liderbet.sportradar.RawMatch;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.boot.context.event.ApplicationStartedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;

@Component
@Slf4j
public class KafkaListener {

    private final ReactiveKafkaConsumerTemplate<String, RawMatch> reactiveKafkaConsumerTemplate;


    public KafkaListener(ReactiveKafkaConsumerTemplate<String, RawMatch> reactiveKafkaConsumerTemplate) {
        this.reactiveKafkaConsumerTemplate = reactiveKafkaConsumerTemplate;
    }


    @EventListener(ApplicationStartedEvent.class)
    public Flux<RawMatch> startKafkaConsumer() {
        return reactiveKafkaConsumerTemplate
                .receiveAutoAck()
                .doOnNext(consumerRecord -> log.info("received key={}, value={} from topic={}, offset={}",
                        consumerRecord.key(),
                        consumerRecord.value(),
                        consumerRecord.topic(),
                        consumerRecord.offset())
                )
                .map(ConsumerRecord::value)
                .doOnNext(match -> log.info("successfully consumed {}={}", RawMatch.class.getSimpleName(), match))
                .doOnError(throwable -> log.error("something bad happened while consuming : {}", throwable.getMessage()));
    }

}
