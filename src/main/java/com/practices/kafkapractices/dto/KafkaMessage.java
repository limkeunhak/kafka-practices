package com.practices.kafkapractices.dto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.practices.kafkapractices.dto.TestDTO;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;


@Getter
@Setter
@ToString
public class KafkaMessage {
    public String event_type;
    public Object event_message;

    public TestDIO getDtoMessage() {
        ObjectMapper mapper = new ObjectMapper();
        messageObject.event_message = mapper.convertValue(messageObject.event_message, TestDTO.class);

        return this.event_message;
    }
}
