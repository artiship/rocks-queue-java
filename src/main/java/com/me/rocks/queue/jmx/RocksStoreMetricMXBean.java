package com.me.rocks.queue.jmx;

public interface RocksStoreMetricMXBean {
    String getDatabaseName();
    String getRocksdbLocation();
    long getRocksDBDiskUsageInBytes();
    int getNumberOfQueueCreated();
    boolean getIsOpen();
    boolean getIsClosed();
    void reset();
}
