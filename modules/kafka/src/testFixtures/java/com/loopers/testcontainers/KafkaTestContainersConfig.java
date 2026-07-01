package com.loopers.testcontainers;

import org.springframework.context.annotation.Configuration;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

@Configuration
public class KafkaTestContainersConfig {

    private static final KafkaContainer kafkaContainer;

    static {
        kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.0"));
        kafkaContainer.start();

        System.setProperty("spring.kafka.bootstrap-servers", kafkaContainer.getBootstrapServers());
        System.setProperty("spring.kafka.admin.properties[bootstrap.servers]", kafkaContainer.getBootstrapServers());
        System.setProperty("spring.kafka.consumer.auto-offset-reset", "earliest");
    }
}
