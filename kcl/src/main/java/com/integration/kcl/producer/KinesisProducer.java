package com.integration.kcl.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.apache.commons.lang3.BooleanUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.services.kinesis.KinesisClient;
import software.amazon.awssdk.services.kinesis.model.PutRecordRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequest;
import software.amazon.awssdk.services.kinesis.model.PutRecordsRequestEntry;

import java.util.*;
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

    private final KinesisClient kinesisClient;

    private static final String dummyDataTemplate = """
            {
            "id": {id}, "binary": {bin}
            }
            """;

    public void addEvent(String partitionKey, String data) {
        var putRecordRequest = PutRecordRequest.builder()
                .partitionKey(partitionKey)
                .streamName(streamName)
                .data(SdkBytes.fromByteArray(data.getBytes()))
                .build();

        kinesisClient.putRecord(putRecordRequest);
        log.info("Added into Kinesis...");
    }

    public void addEvents(Map<String, String> data) {
        var putRecordsRequestList = data.entrySet().stream().map(entry ->
                PutRecordsRequestEntry.builder()
                        .partitionKey(entry.getKey())
                        .data(SdkBytes.fromByteArray(entry.getValue().getBytes()))
                        .build()).toList();
        kinesisClient.putRecords(PutRecordsRequest.builder()
                .records(putRecordsRequestList)
                .streamName(streamName)
                .build());
        log.info("Added batch into Kinesis...");
    }

    @Scheduled(fixedRate = 60000)
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
            kinesisClient.putRecord(request);
            log.info("Added into Kinesis...");
        });
    }

    private void addDummyEventsInBatches() {
        var putRecordRequestEntries = new ArrayList<PutRecordsRequestEntry>();
        IntStream.range(0, 500).forEach(itr -> {
            var randNum = new Random().nextInt(39) + 1;
            var pk = String.valueOf(randNum);
            var data = dummyDataTemplate.replace("{id}", pk).replace("{bin}", Integer.toBinaryString(randNum));
            putRecordRequestEntries.add(PutRecordsRequestEntry.builder()
                    .partitionKey(pk)
                    .data(SdkBytes.fromByteArray(data.getBytes()))
                    .build());
        });
        kinesisClient.putRecords(PutRecordsRequest.builder().streamName(streamName).records(putRecordRequestEntries).build());
        log.info("Added batch into Kinesis...");
    }
}
