package com.example.demokafkaproducer;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.CompletableFuture;

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
    public CompletableFuture<String> sendMessage(@RequestBody String message) {
        CompletableFuture<SendResult<Object, String>> future = this.kafkaTemplate.send(topic, message).completable();
        return future.thenApply(result -> result.getProducerRecord().toString());
    }


    public static void main(String[] args) {
        SpringApplication.run(DemoKafkaProducerApplication.class, args);
    }
}
