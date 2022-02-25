package com.nttdata.poc;

import org.junit.jupiter.api.Test;

import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class MonitorTest {

    @Test
    public void throughput() throws InterruptedException {
        Monitor m = Monitor.createForTest();
        IntStream.range(1, 100).forEach(i -> m.addMessageProcessed());
        Thread.sleep(2000L);
        assertThat(m.getEnrichedThroughputMessagePerSecond()).isGreaterThan(40L);

        // The next call, without messages, should return 0
        assertThat(m.getEnrichedThroughputMessagePerSecond()).isEqualTo(0L);
    }
}