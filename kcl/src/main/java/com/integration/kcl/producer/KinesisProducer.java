package com.integration.kcl.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.BooleanUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisAsyncClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.stream.IntStream;

/**
 * @author mohit.rawat
 */
@Log4j2
@Component
@RequiredArgsConstructor
public class KinesisProducer {

    @Value("${aws.kinesis.streamName}")
    private String streamName;

    @Value("${spring.application.batchingEnabled}")
    private Boolean batchingEnabled;

    private final KinesisAsyncClient kinesisAsyncClient;

    private static final String dummyDataTemplate = """
            {
            "id": {id}, "binary": {bin}
            }
            """;

    public boolean addEvent(String partitionKey, String data) {
        var putRecordRequest = PutRecordRequest.builder()
                .partitionKey(partitionKey)
                .streamName(streamName)
                .data(SdkBytes.fromByteArray(data.getBytes()))
                .build();
        try {
            kinesisAsyncClient.putRecord(putRecordRequest).get();
            log.info("Added into Kinesis...");
        } catch (InterruptedException | ExecutionException ex) {
            log.error("Exception occurred. Ex: ", ex);
            return false;
        }
        return true;
    }

    @Scheduled(fixedRate = 120000)
    public void scheduledProducer() {
        if (BooleanUtils.isTrue(batchingEnabled)) {
            addDummyEventsInBatches();
        } else {
            addDummyEvents();
        }
    }

    private void addDummyEvents() {
        IntStream.range(0, 500).forEach(itr -> {
            var randNum = new Random().nextInt(39) + 1;
            var pk = String.valueOf(randNum);
            var data = dummyDataTemplate.replace("{id}", pk).replace("{bin}", Integer.toBinaryString(randNum));
            var request = PutRecordRequest.builder()
                    .streamName(streamName)
                    .partitionKey(pk)
                    .data(SdkBytes.fromByteArray(data.getBytes()))
                    .build();
            try {
                kinesisAsyncClient.putRecord(request).get();
                log.info("Added into Kinesis...");
            } catch (InterruptedException | ExecutionException ex) {
                log.error("Exception occurred. Ex: ", ex);
            }
        });
    }

    private void addDummyEventsInBatches() {
        var putRecordsRequestList = new ArrayList<PutRecordsRequestEntry>();

        IntStream.range(0, 500).forEach(itr -> {
            var randNum = new Random().nextInt(39) + 1;
            var pk = String.valueOf(randNum);
            var data = dummyDataTemplate.replace("{id}", pk).replace("{bin}", Integer.toBinaryString(randNum));
            var request = PutRecordsRequestEntry.builder()
                    .partitionKey(pk)
                    .data(SdkBytes.fromByteArray(data.getBytes()))
                    .build();
            putRecordsRequestList.add(request);
        });

        try {
            kinesisAsyncClient.putRecords(PutRecordsRequest.builder()
                    .records(putRecordsRequestList)
                    .streamName(streamName)
                    .build()).get();
            log.info("Added batch into Kinesis...");
        } catch (InterruptedException | ExecutionException ex) {
            log.error("Exception occurred. Ex: ", ex);
        }
    }
}
