package com.example.demokafkaproducer;

import java.util.Map;

import org.apache.kafka.clients.producer.RecordMetadata;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.SenderOptions;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@SpringBootApplication
@RestController
public class DemoKafkaProducerApplication {
	private final String topic;

	private final ReactiveKafkaProducerTemplate<Object, String> kafkaTemplate;

	public DemoKafkaProducerApplication(@Value("${sample.topic}") String topic, KafkaProperties properties) {
		this.topic = topic;
		this.kafkaTemplate = new ReactiveKafkaProducerTemplate<>(SenderOptions.create(properties.buildProducerProperties()));
	}

	@PostMapping(path = "/messages")
	public Mono<Map<String, ?>> sendMessage(@RequestBody String message) {
		return this.kafkaTemplate.send(topic, message)
				.map(result -> {
					final RecordMetadata recordMetadata = result.recordMetadata();
					return Map.of(
							"topic", recordMetadata.topic(),
							"partition", recordMetadata.partition(),
							"offset", recordMetadata.offset());
				});
	}

	public static void main(String[] args) {
		SpringApplication.run(DemoKafkaProducerApplication.class, args);
	}
}
