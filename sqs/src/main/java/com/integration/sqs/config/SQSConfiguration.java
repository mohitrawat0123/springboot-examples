package com.integration.sqs.config;


import io.awspring.cloud.sqs.MessageExecutionThreadFactory;
import io.awspring.cloud.sqs.config.SqsMessageListenerContainerFactory;
import io.awspring.cloud.sqs.listener.acknowledgement.handler.AcknowledgementMode;
import lombok.RequiredArgsConstructor;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import software.amazon.awssdk.services.sqs.SqsAsyncClient;

import java.time.Duration;

/**
 * @author mohitrawat0123
 */
@Configuration
@RequiredArgsConstructor
public class SQSConfiguration {

    @Value("${aws.region.static}")
    private String awsRegion;

    private final SqsAsyncClient sqsAsyncClient;

    @Bean
    public SqsMessageListenerContainerFactory<Object> defaultSqsListenerContainerFactory() {
        return SqsMessageListenerContainerFactory
                .builder()
                .sqsAsyncClient(sqsAsyncClient)
                .configure(options -> options
                        .componentsTaskExecutor(getTaskExecutor())
                        .maxMessagesPerPoll(10)
                        .messageVisibility(Duration.ofSeconds(20))
                        .pollTimeout(Duration.ofSeconds(10))
                        .acknowledgementMode(AcknowledgementMode.ON_SUCCESS)
                        .build()
                ).build();
    }

    private ThreadPoolTaskExecutor getTaskExecutor() {
        MessageExecutionThreadFactory factory = new MessageExecutionThreadFactory();
        factory.setThreadNamePrefix("SQSExecutor - ");

        ThreadPoolTaskExecutor taskExecutor = new ThreadPoolTaskExecutor();
        taskExecutor.setCorePoolSize(10);
        taskExecutor.setMaxPoolSize(20);
        taskExecutor.setQueueCapacity(10);
        taskExecutor.setThreadFactory(factory);
        taskExecutor.afterPropertiesSet();
        taskExecutor.setTaskDecorator(runnable -> {
            var contextMap = MDC.getCopyOfContextMap();
            return () -> {
                try {
                    MDC.setContextMap(contextMap);
                    runnable.run();
                } finally {
                    MDC.clear();
                }
            };
        });
        return taskExecutor;
    }

}