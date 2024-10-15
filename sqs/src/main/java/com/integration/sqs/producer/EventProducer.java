package com.integration.sqs.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.MessageAttributeValue;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageRequest;

import java.util.HashMap;

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

    private final SqsClient sqsClient;

    private final SnsClient snsClient;

    public void sendMessage(String message) {

        var sendMsgRequest = SendMessageRequest.builder().queueUrl(queueURL).messageBody(message).build();
        var response = sqsClient.sendMessage(sendMsgRequest);
        log.info("Message sent successfully. MessageId: {}", response.messageId());

    }

    public void publishMessage(String message) {
        var messageAttributes = new HashMap<String, MessageAttributeValue>() { {
            put("attr1", MessageAttributeValue.builder().stringValue("val1").dataType("String").build());
            put("attr2", MessageAttributeValue.builder().stringValue("val2").dataType("String").build());
        } };

        var publishMsgRequest = PublishRequest.builder()
                .topicArn(topicArn)
                .message(message)
                .messageAttributes(messageAttributes)
                .build();

        var response = snsClient.publish(publishMsgRequest);
        log.info("Message published successfully. MessageId: {}", response.messageId());
    }
}
