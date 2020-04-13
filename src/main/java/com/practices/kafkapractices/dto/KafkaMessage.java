package com.practices.kafkapractices.dto;

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
