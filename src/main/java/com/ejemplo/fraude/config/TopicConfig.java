package com.ejemplo.fraude.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class TopicConfig {

    @Bean
    public NewTopic transaccionesTopic() {
        return TopicBuilder.name("transacciones-bancarias")
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic alertasFraudeTopic() {
        return TopicBuilder.name("alertas-fraude")
                .partitions(3)
                .replicas(1)
                .build();
    }
}