package com.nttdata.poc;

public interface MonitorMBean {

    void addRebalance();

    Integer getRebalance();

    void addDlqMessage();

    Integer getDlqMessages();

    void addMessageProcessed();

    Long getMessageProcessed();

    Long getEnrichedThroughputMessagePerSecond();

    Long getUptimeSeconds();
}
