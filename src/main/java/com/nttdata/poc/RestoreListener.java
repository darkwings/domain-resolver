package com.nttdata.poc;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.streams.processor.StateRestoreListener;

@Slf4j
class RestoreListener implements StateRestoreListener {

    @Override
    public void onRestoreStart(TopicPartition tp, String storeName,
                               long startingOffset, long endingOffset) {
        log.warn("Restore started on store {} (tp {})", storeName, tp);
    }

    @Override
    public void onBatchRestored(TopicPartition tp, String storeName,
                                long batchEndOffset, long numRestored) {
    }

    @Override
    public void onRestoreEnd(TopicPartition tp, String storeName,
                             long totalRestored) {
        log.warn("Restore ended on store {} (tp {}). Restored {}", storeName, tp, totalRestored);
    }
}
