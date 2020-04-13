package com.practices.kafkapractices.examples;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.practices.kafkapractices.dto.KafkaMessage;
import com.practices.kafkapractices.wrapped.BaseKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Component;

@Component
public class MemberKafkaConsumer implements BaseKafkaConsumer {

    @Override
    public void onConsumeMessage(String message, Iterable headers) throws Exception {
        // WRITE YOUR BUSINESS CODE HERE
    }

    @Override
    public void onError(Exception ex, ConsumerRecord record) {
        // WRITE YOUR EXCEPTION HANDLING CODE HERE
    }

    // JSON STRING TO OBJECT CONVERSION EXAMPLE
    private KafkaMessage convertMessageType(String jsonInString) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(jsonInString, KafkaMessage.class);
    }
}
