package com.me.rocks.queue.jmx;

import com.me.rocks.queue.RocksQueue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class RocksQueueMetrics extends RocksMetrics implements RocksQueueMetricMXBean {

    private final RocksQueue rocksQueue;

    private final AtomicBoolean isQueueCreated = new AtomicBoolean();
    private final AtomicBoolean isQueueClosed = new AtomicBoolean();
    private final AtomicLong timestampOfLastEqueue = new AtomicLong();
    private final AtomicLong timestampOfLastConsume = new AtomicLong();
    private final AtomicLong timestampOfLastDequeue = new AtomicLong();
    private final AtomicLong queueAccumulateBytes = new AtomicLong();

    public RocksQueueMetrics(RocksQueue rocksQueue) {
        this.rocksQueue = rocksQueue;
        rocksQueue.registerStatisticsListener(this);
    }

    @Override
    protected String getObjectName() {
        return new StringBuilder("rocks.queue:type=")
                .append(this.getClass())
                .append(",")
                .append("queue=")
                .append(getQueueName())
                .toString();
    }

    @Override
    public String getQueueName() {
        return rocksQueue.getQueueName();
    }

    @Override
    public long getQueueSize() {
        return rocksQueue.getSize();
    }

    @Override
    public long getQueueAccumulateBytes() {
        return this.queueAccumulateBytes.get();
    }

    @Override
    public long getQueueHeadIndex() {
        return rocksQueue.getHeadIndex();
    }

    @Override
    public long getQueueTailIndex() {
        return rocksQueue.getTailIndex();
    }

    @Override
    public boolean getIsQueueCreated() {
        return isQueueCreated.get();
    }

    @Override
    public boolean getIsQueueClosed() {
        return isQueueClosed.get();
    }

    @Override
    public long getTimestampOfLastEqueue() {
        return timestampOfLastEqueue.get();
    }

    @Override
    public long getTimestampOfLastConsume() {
        return timestampOfLastConsume.get();
    }

    @Override
    public long getTimestampOfLastDequeue() {
        return timestampOfLastDequeue.get();
    }

    public void onInitialize() {
        this.isQueueCreated.set(true);
        this.isQueueClosed.set(false);
    }

    public void onClose() {
        this.isQueueClosed.set(true);
    }

    public void onEnqueue(long size) {
        this.timestampOfLastEqueue.set(currentTimestamp());
        this.queueAccumulateBytes.addAndGet(size);
    }

    public void onDequeue(long size) {
        this.timestampOfLastDequeue.set(currentTimestamp());
        this.queueAccumulateBytes.addAndGet(-size);
    }

    public void onConsume() {
        this.timestampOfLastConsume.set(currentTimestamp());
    }

    @Override
    public void reset() {
        this.isQueueClosed.set(false);
        this.isQueueCreated.set(false);
        this.timestampOfLastConsume.set(0);
        this.timestampOfLastEqueue.set(0);
        this.timestampOfLastDequeue.set(0);
    }
}
