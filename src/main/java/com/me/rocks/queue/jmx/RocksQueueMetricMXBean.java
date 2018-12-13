package com.me.rocks.queue.jmx;

public interface RocksQueueMetricMXBean {
    String getQueueName();
    long getQueueSize();
    long getAccumulateBytes();
    long getHeadIndex();
    long getTailIndex();
    boolean getIsCreated();
    boolean getIsClosed();

    long getSecondsSinceLastEnqueue();
    long getSecondsSinceLastConsume();
    long getSecondsSinceLastDequeue();
    void reset();
}
