package com.practices.kafkapractices.wrapped;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.practices.kafkapractices.aspects.AutoCommitOffset;
import com.practices.kafkapractices.aspects.LogExecutionTime;
import com.practices.kafkapractices.aspects.ValidateInputMessage;
import com.practices.kafkapractices.dto.KafkaMessage;
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

//    @AutoCommitOffset
//    @ValidateInputMessage
//    @LogExecutionTime
    @KafkaListener(topics = "test", groupId = "test-consumer-string")
    private void consume(String message) {
        try {
            if (!this.isJSONValid(message)) {
                throw new IllegalArgumentException("Kafka message is not valid json string.");
            } else if (!this.hasValidFields(message)) {
                throw new IllegalArgumentException("Kafka message format is not valid.");
            }

            ObjectMapper mapper = new ObjectMapper();
            KafkaMessage messageObject = mapper.readValue(message, KafkaMessage.class);
            onConsumeMessage(messageObject);
        } catch (Exception ex) {
            logger.error(ex.getMessage());
            onError(ex);
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

    private boolean hasValidFields(String jsonInString) {
        try {
            final ObjectMapper mapper = new ObjectMapper();
            KafkaMessage message = mapper.readValue(jsonInString, KafkaMessage.class);

            if(message.event_message == "" || message.event_type == "") {
                return false;
            }
            return true;
        } catch (Exception ex) {
            return false;
        }
    }

    public void onConsumeMessage(KafkaMessage message);
    public void onError(Exception ex);
}
