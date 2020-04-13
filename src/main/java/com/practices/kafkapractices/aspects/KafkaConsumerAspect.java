package com.practices.kafkapractices.aspects;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.support.Acknowledgment;
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
    private Logger logger = LoggerFactory.getLogger(KafkaConsumerAspect.class);

    private static String ILLEGAL_NUMBER_OF_ARGUMENT_EXCEPTION_TEXT = "the comsume method must have 2 arguments.";
    private static String ILLEGAL_MESSAGE_FORMAT_EXCEPTION_TEXT = "the kafka message value is not valid json string.";

    /**
     * KafkaListener를 통해 전달받은 메시지의 처리 후 메시지 오프셋을 커밋합니다.
     *
     * @param joinPoint Unused.
     * @return Nothing.
     * @exception IOException On input error.
     * @see IOException
     */
    @After("@annotation(CommitOffsetEveryMessage)")
    public void PostConsumeAdvice(JoinPoint joinPoint) {
        try {
            if (joinPoint.getArgs().length != 2) {
                throw new IllegalArgumentException(ILLEGAL_NUMBER_OF_ARGUMENT_EXCEPTION_TEXT);
            }

            Acknowledgment ack = (Acknowledgment) joinPoint.getArgs()[1];

            ack.acknowledge();
        } catch(ClassCastException ex) {
            logger.error(ex.getMessage());
        } catch(IllegalArgumentException ex) {
            logger.error(ex.getMessage());
        }
    }

    /**
     * KafkaListener를 통해 전달받은 메시지가 유효한 JSON 문자열인지 확인합니다.
     *
     * @param joinPoint Unused.
     * @return Nothing.
     * @exception IOException On input error.
     * @see IOException
     */
    @After("@annotation(ValidateMessageFormat)")
    public void PreConsumeAdvice(JoinPoint joinPoint) throws IllegalArgumentException {
        String message = joinPoint.getArgs()[0].toString();

        if (!this.isJSONValid(message)) {
            throw new IllegalArgumentException(ILLEGAL_MESSAGE_FORMAT_EXCEPTION_TEXT);
        }
    }

    @AfterThrowing( value = "@annotation(HandleMessageException)", throwing = "throwable")
    public void ExceptionHandleAdvice(JoinPoint joinPoint, Throwable throwable) throws IllegalArgumentException {
        // WRITE YOUR EXCEPTION HANDLING CODE HERE
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


    private boolean isJSONValid(String jsonInString) {
        try {
            final ObjectMapper mapper = new ObjectMapper();
            mapper.readTree(jsonInString);
            return true;
        } catch (IOException e) {
            return false;
        }
    }
}
