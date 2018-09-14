package com.me.rocks.queue.jmx;

interface RocksQueueMetricMXBean {
    String getQueueName();
    long getQueueSize();
    long getQueueAccumulateBytes();
    long getQueueHeadIndex();
    long getQueueTailIndex();
    boolean getIsQueueCreated();
    boolean getIsQueueClosed();
    long getTimestampOfLastEqueue();
    long getTimestampOfLastConsume();
    long getTimestampOfLastDequeue();
    void reset();
}
