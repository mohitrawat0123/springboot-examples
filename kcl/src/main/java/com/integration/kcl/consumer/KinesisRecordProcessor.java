package com.integration.kcl.consumer;

import lombok.extern.log4j.Log4j2;
import software.amazon.kinesis.lifecycle.events.*;
import software.amazon.kinesis.processor.ShardRecordProcessor;
import software.amazon.kinesis.retrieval.KinesisClientRecord;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * @author mohit.rawat
 */
@Log4j2
public class KinesisRecordProcessor implements ShardRecordProcessor {

    @Override
    public void initialize(InitializationInput initializationInput) {
        log.info("Initializing record processor for shard: {}", initializationInput.shardId());
    }

    @Override
    public void processRecords(ProcessRecordsInput processRecordsInput) {
        List<KinesisClientRecord> records = processRecordsInput.records();
        log.info("Processing {} record(s)", records.size());

        records.forEach(record -> {
            String data = StandardCharsets.UTF_8.decode(record.data()).toString();
            log.info("Processing record pk: {} -- Seq: {} \n data: {}",
                    record.partitionKey(), record.sequenceNumber(), data);
        });

        try {
            processRecordsInput.checkpointer().checkpoint();
        } catch (Exception e) {
            log.error("Exception while checkpointing after processing records. Giving up.", e);
        }
    }

    @Override
    public void leaseLost(LeaseLostInput leaseLostInput) {
        log.info("Lease lost. Processing will be taken over by another worker.");
    }

    @Override
    public void shardEnded(ShardEndedInput shardEndedInput) {
        try {
            log.info("Reached shard end checkpointing.");
            shardEndedInput.checkpointer().checkpoint();
        } catch (Exception e) {
            log.error("Exception while checkpointing at shard end. Giving up.", e);
        }
    }

    @Override
    public void shutdownRequested(ShutdownRequestedInput shutdownRequestedInput) {
        try {
            log.info("Scheduler is shutting down, checkpointing.");
            shutdownRequestedInput.checkpointer().checkpoint();
        } catch (Exception e) {
            log.error("Exception while checkpointing at requested shutdown. Giving up.", e);
        }
    }
}
