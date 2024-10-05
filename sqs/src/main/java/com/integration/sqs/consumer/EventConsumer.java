package com.integration.sqs.consumer;

import io.awspring.cloud.sqs.annotation.SqsListener;
import lombok.extern.log4j.Log4j2;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

/**
 * @author mohitrawat0123
 */
@Log4j2
@Component
public class EventConsumer {

    @SqsListener("${aws.sqs.name}")
    public void eventConsumer(String rawMessage) {
        log.info("Message received: {}", rawMessage);
    }
}
