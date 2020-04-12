package com.practices.kafkapractices.basic;

import com.practices.kafkapractices.aspects.HandleConsumerError;
import com.practices.kafkapractices.aspects.ValidateInputMessage;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class BasicConsumerUsage {

    @KafkaListener(topics = "cat", groupId = "test-consumer-record")
    public void consume(ConsumerRecord<String, String> message) {       // Get ConsumerRecord.
        System.out.println(message.toString());
        System.out.println(message.getClass());
    }

    @HandleConsumerError
    @ValidateInputMessage
    @KafkaListener(topics = "cat", groupId = "test-consumer-string")             // Get Message value with String.
    public void consumeOnlyMessage(String message) {
        System.out.println(message);
        System.out.println(message.getClass());
    }

}