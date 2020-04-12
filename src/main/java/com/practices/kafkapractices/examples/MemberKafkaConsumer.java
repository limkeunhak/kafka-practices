package com.practices.kafkapractices.examples;

import com.practices.kafkapractices.dto.KafkaMessage;
import com.practices.kafkapractices.wrapped.BaseKafkaConsumer;
import org.springframework.stereotype.Component;

@Component
public class MemberKafkaConsumer implements BaseKafkaConsumer {

    @Override
    public void onConsumeMessage(KafkaMessage message) {
        System.out.println(message);
    }

    @Override
    public void onError(Exception ex) {
        System.out.println(ex);
    }
}
