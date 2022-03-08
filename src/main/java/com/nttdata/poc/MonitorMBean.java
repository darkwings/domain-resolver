package com.nttdata.poc;

public interface MonitorMBean {

    void addRebalance();

    Integer getRebalance();

    void addDlqMessage();

    void addRetryMessage();

    Integer getDlqMessages();

    Integer getRetryMessages();

    void addMessageProcessed();

    Long getMessageProcessed();

    Long getEnrichedThroughputMessagePerSecond();

    Long getUptimeSeconds();
}
