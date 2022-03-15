package com.nttdata.poc;

import com.google.common.annotations.VisibleForTesting;
import lombok.AccessLevel;
import lombok.ToString;
import lombok.experimental.FieldDefaults;
import lombok.extern.slf4j.Slf4j;
import lombok.val;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

@Slf4j
@FieldDefaults(level = AccessLevel.PRIVATE)
@ToString
public class Monitor implements MonitorMBean {

    private static final long NANOS_TO_SECONDS_RATIO = 1_000_000_000L;

    int rebalance;
    int dlqMessages;
    int retryMessages;
    int cacheHit;
    int cacheMiss;
    long messageProcessed;
    final long startTime;

    long windowStart;
    long windowMessages;

    private Monitor() {
        this.rebalance = 0;
        this.dlqMessages = 0;
        this.retryMessages = 0;
        this.messageProcessed = 0;
        this.windowMessages = 0;
        this.cacheHit = 0;
        this.cacheMiss = 0;
        this.windowStart = System.nanoTime();
        this.startTime = System.nanoTime();
    }

    // TODO singleton!
    public static Monitor create() {
        Monitor m = new Monitor();
        String disable = System.getProperty("disable.jmx");
        if (disable == null) {
            try {
                var objectName = new ObjectName("com.nttdata.poc:type=basic,name=monitor");
                MBeanServer server = ManagementFactory.getPlatformMBeanServer();
                server.registerMBean(m, objectName);
            } catch (Exception e) {
                log.error("Failed to register monitor MBean", e);
            }
        }
        return m;
    }

    @VisibleForTesting
    static Monitor createForTest() {
        return new Monitor();
    }

    @Override
    public void addCacheHit() {
        cacheHit++;
    }

    @Override
    public Integer getCacheHit() {
        return cacheHit;
    }

    @Override
    public void addCacheMiss() {
        cacheMiss++;
    }

    @Override
    public Integer getCacheMiss() {
        return cacheMiss;
    }

    @Override
    public void addRebalance() {
        rebalance++;
    }

    @Override
    public Integer getRebalance() {
        return rebalance;
    }

    @Override
    public void addDlqMessage() {
        dlqMessages++;
    }

    @Override
    public void addRetryMessage() {
        retryMessages++;
    }

    @Override
    public Integer getDlqMessages() {
        return dlqMessages;
    }

    @Override
    public Integer getRetryMessages() {
        return retryMessages;
    }

    @Override
    public void addMessageProcessed() {
        messageProcessed++;
        windowMessages++;
    }

    @Override
    public Long getMessageProcessed() {
        return messageProcessed;
    }

    @Override
    public Long getUptimeSeconds() {
        return (System.nanoTime() - startTime) / NANOS_TO_SECONDS_RATIO;
    }

    @Override
    public Long getEnrichedThroughputMessagePerSecond() {
        try {
            val windowSec = (System.nanoTime() - windowStart) / NANOS_TO_SECONDS_RATIO;
            val count = windowSec != 0 ? windowMessages / windowSec : 0L;

            // TODO meglio, così è solo abbozzato
            windowStart = System.nanoTime(); // Restart the window
            windowMessages = 0;
            return count;
        }
        catch (Exception e) {
            return 0L;
        }
    }
}
