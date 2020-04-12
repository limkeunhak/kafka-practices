package com.practices.kafkapractices.examples;

import com.practices.kafkapractices.dto.KafkaMessage;
import com.practices.kafkapractices.dto.TestDTO;
import com.practices.kafkapractices.wrapped.BaseKafkaConsumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.aspectj.weaver.ast.Test;
import org.springframework.stereotype.Component;

@Component
public class MemberKafkaConsumer implements BaseKafkaConsumer {

    @Override
    public void onConsumeMessage(KafkaMessage message) {
        System.out.println(message);
        System.out.println(message.event_message.getClass());
    }

    @Override
    public void onError(Exception ex, ConsumerRecord record) {
        System.out.println(ex);
    }
}
