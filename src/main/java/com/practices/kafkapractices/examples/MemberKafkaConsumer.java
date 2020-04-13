package com.practices.kafkapractices.examples;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.practices.kafkapractices.dto.KafkaMessage;
import com.practices.kafkapractices.dto.TestDTO;
import com.practices.kafkapractices.wrapped.BaseKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.aspectj.weaver.ast.Test;
import org.springframework.stereotype.Component;

@Component
public class MemberKafkaConsumer implements BaseKafkaConsumer {

    @Override
    public void onConsumeMessage(KafkaMessage message) throws Exception {
        System.out.println(message);
        System.out.println(this.convertMessageType(message.event_message));
    }

    @Override
    public void onError(Exception ex, ConsumerRecord record) {
        System.out.println(ex);
    }


    private TestDTO convertMessageType(Object messageObject) throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.convertValue(messageObject, TestDTO.class);
    }
}
