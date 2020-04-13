package com.practices.kafkapractices.basic;

import org.springframework.stereotype.Component;

@Component
public class BasicConsumerUsage {

//    @KafkaListener(topics = "cat", groupId = "test-consumer-record")
//    public void consume(ConsumerRecord<String, String> message) {       // Get ConsumerRecord.
//        System.out.println(message.toString());
//        System.out.println(message.getClass());
//    }

//    @HandleConsumerError
//    @ValidateInputMessage
//    @KafkaListener(topics = "cat", groupId = "test-consumer-string")             // Get Message value with String.
//    public void consumeOnlyMessage(String message) {
//        System.out.println(message);
//        System.out.println(message.getClass());
//    }

}