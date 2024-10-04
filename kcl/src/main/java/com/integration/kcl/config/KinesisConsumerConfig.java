package com.integration.kcl.config;

import com.integration.kcl.consumer.KinesisRecordProcessorFactory;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import software.amazon.awssdk.services.cloudwatch.CloudWatchAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.kinesis.common.ConfigsBuilder;
import software.amazon.kinesis.coordinator.Scheduler;
import software.amazon.kinesis.metrics.MetricsLevel;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


/**
 * @author mohit.rawat
 */
@Log4j2
@Configuration
@RequiredArgsConstructor
public class KinesisConsumerConfig {

    @Value("${aws.kinesis.streamName}")
    private String streamName;

    @Value("${aws.kinesis.consumerName}")
    private String consumerName;

    private final KinesisAsyncClient kinesisAsyncClient;
    private final DynamoDbAsyncClient dynamoDbAsyncClient;
    private final CloudWatchAsyncClient cloudWatchAsyncClient;

    @PostConstruct
    public void initKinesisClient() {

        ConfigsBuilder configsBuilder = new ConfigsBuilder(
                streamName,
                consumerName,
                kinesisAsyncClient,
                dynamoDbAsyncClient,
                cloudWatchAsyncClient,
                UUID.randomUUID().toString(),
                new KinesisRecordProcessorFactory());

        var coordinatorConfig = configsBuilder.coordinatorConfig()
                .maxInitializationAttempts(5)
                .schedulerInitializationBackoffTimeMillis(5000L);

        var scheduler = new Scheduler(
                configsBuilder.checkpointConfig(),
                coordinatorConfig,
                configsBuilder.leaseManagementConfig(),
                configsBuilder.lifecycleConfig(),
                configsBuilder.metricsConfig().metricsLevel(MetricsLevel.DETAILED),
                configsBuilder.processorConfig(),
                configsBuilder.retrievalConfig()
        );

        ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(scheduler);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            scheduler.shutdown();
            executorService.shutdown();
        }));
    }
}
