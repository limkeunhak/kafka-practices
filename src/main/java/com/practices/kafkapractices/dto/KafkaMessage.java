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
    public Object event_message;
}
