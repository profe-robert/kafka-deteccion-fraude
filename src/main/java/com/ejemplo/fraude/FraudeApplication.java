package com.ejemplo.fraude;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class FraudeApplication {

	public static void main(String[] args) {
		SpringApplication.run(FraudeApplication.class, args);
	}

}
