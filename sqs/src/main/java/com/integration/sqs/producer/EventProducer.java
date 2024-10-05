package com.integration.sqs.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sns.SnsAsyncClient;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.util.HashMap;
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

    @Value("${aws.sns.arn}")
    private String topicArn;

    private final SqsAsyncClient sqsAsyncClient;

    private final SnsAsyncClient snsAsyncClient;

    public void sendMessage(String message) {
        try {
            var sendMsgRequest = SendMessageRequest.builder().queueUrl(queueURL).messageBody(message).build();
            sqsAsyncClient.sendMessage(sendMsgRequest).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Exception occurred. Err: ", e);
        }
    }

    public void publishMessage(String message) {
        try {
            var messageAttributes = new HashMap<String, MessageAttributeValue>() { {
                put("attr1", MessageAttributeValue.builder().stringValue("val1").dataType("String").build());
                put("attr2", MessageAttributeValue.builder().stringValue("val2").dataType("String").build());
            } };
            var publishMsgRequest = PublishRequest.builder()
                    .topicArn(topicArn)
                    .message(message)
                    .messageAttributes(messageAttributes)
                    .build();
            snsAsyncClient.publish(publishMsgRequest).get();
        } catch (InterruptedException | ExecutionException e) {
            log.error("Exception occurred. Err: ", e);
        }
    }
}
