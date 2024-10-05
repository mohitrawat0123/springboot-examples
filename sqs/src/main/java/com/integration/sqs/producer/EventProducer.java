package com.integration.sqs.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.util.concurrent.ExecutionException;

/**
 * @author mohitrawat0123
 */
@Log4j2
@Component
@RequiredArgsConstructor
public class EventProducer {

    @Value("${aws.sqs.url}")
    private String queueURL;

    private final SqsAsyncClient sqsAsyncClient;

    public void sendMessage(String message) {
        try {
            var sendMsgRequest = SendMessageRequest.builder().queueUrl(queueURL).messageBody(message).build();
            sqsAsyncClient.sendMessage(sendMsgRequest).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Exception occurred. Err: ", e);
        }
    }
}
