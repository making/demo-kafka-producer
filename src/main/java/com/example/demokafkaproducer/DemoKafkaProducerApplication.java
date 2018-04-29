package com.example.demokafkaproducer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;

@SpringBootApplication
@RestController
public class DemoKafkaProducerApplication {
    private final String topic;
    private final KafkaTemplate<Object, String> kafkaTemplate;

    public DemoKafkaProducerApplication(
            @Value("${sample.topic}")
                    String topic, KafkaTemplate<Object, String> kafkaTemplate) {
        this.topic = topic;
        this.kafkaTemplate = kafkaTemplate;
    }

    @PostMapping(path = "/messages")
    public Mono<String> sendMessage(@RequestBody String message) {
        return Mono.fromFuture(this.kafkaTemplate.send(topic, message).completable()) //
                .map(result -> result.getProducerRecord().toString());
    }


    public static void main(String[] args) {
        SpringApplication.run(DemoKafkaProducerApplication.class, args);
    }
}
