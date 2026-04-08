package com.example.kafkadr;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling
public class KafkaDrExampleApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaDrExampleApplication.class, args);
    }
}
