package com.practices.kafkapractices.wrapped;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.practices.kafkapractices.aspects.AutoCommitOffset;
import com.practices.kafkapractices.aspects.HandleConsumerError;
import com.practices.kafkapractices.aspects.ValidateInputMessage;
import com.practices.kafkapractices.dto.KafkaMessage;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.lang.reflect.Type;

public interface BaseKafkaConsumer {

    @AutoCommitOffset
    @HandleConsumerError
    @ValidateInputMessage
    @KafkaListener(topics = "test", groupId = "test-consumer-string")
    private void consume(String message) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        KafkaMessage messageObject = mapper.readValue(message, KafkaMessage.class);
        consumeMessage(messageObject);
    }

    public void consumeMessage(KafkaMessage message);
}
