package com.practices.kafkapractices.aspects;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.practices.kafkapractices.dto.KafkaMessage;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.*;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * KafkaListener에 활용 가능한 공통 Aspect 입니다.
 *
 * @author  Keunhak Lim
 * @version 1.0
 * @since   2020-04-10
 */

@Aspect
@Component
public class KafkaConsumerAspect {

    // TODO: Custom Logger로 변경 후 Autowired 처리
    private Logger logger = LogManager.getLogger();

    /**
     * KafkaListener를 통해 전달받은 메시지의 유효성을 검사합니다.
     * 올바른 형식이 아니라면 IllegalArgumentException
     *
     * @param joinPoint joint point.
     * @return Nothing.
     * @exception IllegalArgumentException On input validation error.
     * @see IllegalArgumentException
     */
    @Before("@annotation(ValidateInputMessage)")
    public void PreConsumeAdvice(JoinPoint joinPoint) throws IllegalArgumentException {
        String message = joinPoint.getArgs()[0].toString();

        if (!this.isJSONValid(message)) {
            logger.error("Kafka message is not valid json string.");
            throw new IllegalArgumentException("Kafka message is not valid json string.");
        } else if (!this.hasValidFields(message)) {
            logger.error("Kafka message format is not valid.");
            throw new IllegalArgumentException("Kafka message format is not valid.");
        }
    }

    /**
     * KafkaListener를 통해 전달받은 메시지의 처리 후 메시지 오프셋을 커밋합니다.
     *
     * @param joinPoint Unused.
     * @return Nothing.
     * @exception IOException On input error.
     * @see IOException
     */
    @After("@annotation(AutoCommitOffset)")
    public void PostConsumeAdvice(JoinPoint joinPoint) {
        System.out.println("PostConsumeAdvice() called");
        // TODO: Auto Commit
    }

    /**
     * KafkaListener의 메시지 처리 시간을 측정합니다.
     * 디버깅 용도로 사용합니다.
     *
     * @param pjp ProcedingJoinPoint.
     * @return Nothing.
     * @exception IOException On input error.
     * @see IOException
     */
    @Around("@annotation(LogExecutionTime)")
    public Object logExecutionTime(ProceedingJoinPoint pjp) throws Throwable {
        long beforeTime = System.currentTimeMillis();
        Object result = pjp.proceed();
        long afterTime = System.currentTimeMillis();

        long secDiffTime = afterTime - beforeTime;

        logger.info("Message processing time(ms): " + secDiffTime);
        return result;
    }

    /**
     * KafkaListener의 메시지 처리 중 발생한 예외를 핸들링합니다.
     *
     * @param ex Throwable.
     * @return Nothing.
     * @exception IOException On input error.
     * @see IOException
     */
    @AfterThrowing(value="@annotation(HandleConsumerError)", throwing="ex")
    public void afterThrowingAdvice(Throwable ex) {
        logger.error(ex.getMessage());
        // TODO: 메시지 처리 중 발생한 예외에 따라 핸들링 로직 추가
    }

    public boolean isJSONValid(String jsonInString) {
        try {
            final ObjectMapper mapper = new ObjectMapper();
            mapper.readTree(jsonInString);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public boolean hasValidFields(String jsonInString) {
        Gson gson = new Gson();

        try {
            KafkaMessage message = gson.fromJson(jsonInString, KafkaMessage.class);

            if(message.event_message == "" || message.event_type == "") {
                return false;
            }
            return true;
        } catch (Exception ex) {
            return false;
        }
    }
}
