package com.nttdata.poc;

public interface MonitorMBean {

    void addRebalance();

    Integer getRebalance();

    void addDlqMessage();

    void addRetryMessage();

    void addCacheHit();

    void addCacheMiss();

    Integer getDlqMessages();

    Integer getRetryMessages();

    Integer getCacheHit();

    Integer getCacheMiss();

    void addMessageProcessed();

    Long getMessageProcessed();

    Long getEnrichedThroughputMessagePerSecond();

    Long getUptimeSeconds();
}
