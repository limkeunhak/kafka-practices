package com.practices.kafkapractices;

import com.practices.kafkapractices.basic.BasicProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaPracticesApplication {

    public static void main(String[] args) {
        SpringApplication.run(KafkaPracticesApplication.class, args);
        BasicProducer producer = new BasicProducer();
        producer.sendMessage("member-service-topic", "message");
    }

}
