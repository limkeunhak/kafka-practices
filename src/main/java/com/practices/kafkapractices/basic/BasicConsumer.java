package com.practices.kafkapractices.basic;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class BasicConsumer {

    @KafkaListener(topics = "cat", groupId = "test-consumer-record")
    public void consume(ConsumerRecord<String, String> message) {       // Get ConsumerRecord.
        System.out.println(message.toString());
        System.out.println(message.getClass());
    }

    @KafkaListener(topics = "cat", groupId = "test-string")             // Get Message value with String.
    public void consumeOnlyMessage(String message) {
        System.out.println(message);
        System.out.println(message.getClass());
    }
}