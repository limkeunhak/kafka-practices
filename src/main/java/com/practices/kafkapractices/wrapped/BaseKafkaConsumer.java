package com.practices.kafkapractices.wrapped;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.practices.kafkapractices.aspects.AutoCommitOffset;
import com.practices.kafkapractices.aspects.LogExecutionTime;
import com.practices.kafkapractices.aspects.ValidateInputMessage;
import com.practices.kafkapractices.dto.KafkaMessage;
import com.practices.kafkapractices.dto.TestDTO;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.kafka.annotation.KafkaListener;

import java.io.IOException;

/**
 * KafkaListener에 활용 가능한 기본 Kafka Consumer 클래스 입니다.
 *
 * @author  Keunhak Lim
 * @version 1.0
 * @since   2020-04-10
 */
public interface BaseKafkaConsumer {

    // TODO: Custom Logger로 변경 후 Autowired 처리 혹은 Logging 제거 (밖에서 로깅하도록)
    Logger logger = LogManager.getLogger();

    @KafkaListener(topics = "test", groupId = "test-consumer-string")
    private void consume(ConsumerRecord record) {
        String message = record.value().toString();

        try {
            if (!this.isJSONValid(message)) {
                throw new IllegalArgumentException("Kafka message is not valid json string.");
            }

            ObjectMapper mapper = new ObjectMapper();
            KafkaMessage messageObject = mapper.readValue(message, KafkaMessage.class);

            // TODO: Generic 으로 처리 못하나???
            messageObject.event_message = mapper.convertValue(messageObject.event_message, TestDTO.class);

            if (!this.hasValidFields(messageObject)) {
                throw new IllegalArgumentException("Kafka message format is not valid.");
            }

            onConsumeMessage(messageObject);
        } catch (Exception ex) {
            logger.error(ex.getMessage());
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

    private boolean hasValidFields(KafkaMessage messageObject) {
        return (messageObject.event_message != null && messageObject.event_type != null);
    }

    public void onConsumeMessage(KafkaMessage message);
    public void onError(Exception ex, ConsumerRecord record);
}
