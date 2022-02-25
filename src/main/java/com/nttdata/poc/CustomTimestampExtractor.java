package com.nttdata.poc;

import com.nttdata.poc.model.Activity;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.time.Instant;

/**
 * Esempio di Custom TS extractor
 *
 * TODO generalizzare per i messaggi IN
 */
public class CustomTimestampExtractor implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        Activity a = (Activity) record.value();
        if (a != null && a.getTimestamp() != null) {
            return Instant.parse(a.getTimestamp()).toEpochMilli();
        }
        return partitionTime;
    }
}
