package com.practices.kafkapractices.examples;

import com.practices.kafkapractices.aspects.CommitOffsetEveryMessage;
import com.practices.kafkapractices.aspects.HandleMessageException;
import com.practices.kafkapractices.aspects.ValidateMessageFormat;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

@Component
public class MemberKafkaConsumerWithoutInterface {

    @KafkaListener(
            topics = "${memberservice.kafka.topic.name.2}",
            containerFactory = "baseKafkaConsumerContainerFactory"
    )
    @HandleMessageException
    @ValidateMessageFormat
    @CommitOffsetEveryMessage
    public void onConsumeMessage(String message, Acknowledgment ack) throws Exception {
        // WRITE YOUR BUSINESS CODE HERE
    }
}
