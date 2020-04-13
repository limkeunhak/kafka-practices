package com.practices.kafkapractices.wrapped;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.practices.kafkapractices.aspects.CommitOffsetEveryMessage;
import com.practices.kafkapractices.aspects.LogExecutionTime;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * KafkaListener에 활용 가능한 기본 Kafka Consumer 클래스 입니다.
 *
 * @author  Keunhak Lim
 * @version 1.0
 * @since   2020-04-10
 */
public interface BaseKafkaConsumer {
    String INVALID_JSON_MESSAGE_FORMAT_EXCEPTION_MESSAGE = "Kafka message is not valid json string.";

    @KafkaListener(
            topics = "${memberservice.kafka.topic.name}",
            containerFactory = "baseKafkaConsumerContainerFactory"
    )
    private void consume(ConsumerRecord<String, String> record, Acknowledgment ack) {
        String message = record.value();

        try {
            if (!this.isJSONValid(message)) {
                throw new IllegalArgumentException(INVALID_JSON_MESSAGE_FORMAT_EXCEPTION_MESSAGE);
            }

            onConsumeMessage(message, record.headers());

            ack.acknowledge();
        } catch (Exception ex) {
            onError(ex, record);
        }
    }

    private boolean isJSONValid(String jsonInString) {
        try {
            final ObjectMapper mapper = new ObjectMapper();
            mapper.readTree(jsonInString);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    void onConsumeMessage(String message, Iterable<Header> headers) throws Exception;
    void onError(Exception ex, ConsumerRecord record);
}
