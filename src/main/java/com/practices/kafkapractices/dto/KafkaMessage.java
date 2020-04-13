package com.practices.kafkapractices.dto;

import com.fasterxml.jackson.core.JsonProcessingException;
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
    public String event_message;

    public TestDTO getDtoMessage() throws JsonProcessingException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readValue(this.event_message, TestDTO.class);
    }
}
