package com.me.rocks.queue.jmx;

import com.me.rocks.queue.RocksQueue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class RocksQueueMetric extends RocksMetrics implements RocksQueueMetricMXBean {

    private final RocksQueue rocksQueue;
    private final String database;

    private final AtomicBoolean isQueueCreated = new AtomicBoolean();
    private final AtomicBoolean isQueueClosed = new AtomicBoolean();
    private final AtomicLong timestampOfLastEqueue = new AtomicLong();
    private final AtomicLong timestampOfLastConsume = new AtomicLong();
    private final AtomicLong timestampOfLastDequeue = new AtomicLong();
    private final AtomicLong queueAccumulateBytes = new AtomicLong();

    public RocksQueueMetric(RocksQueue rocksQueue, String database) {
        this.rocksQueue = rocksQueue;
        this.database = database;
    }

    @Override
    protected String getObjectName() {
        return new StringBuilder("rocks.queue:database=")
                .append(this.database)
                .append(",")
                .append("queue=")
                .append(rocksQueue.getQueueName())
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
    public long getAccumulateBytes() {
        return this.queueAccumulateBytes.get();
    }

    @Override
    public long getHeadIndex() {
        return rocksQueue.getHeadIndex();
    }

    @Override
    public long getTailIndex() {
        return rocksQueue.getTailIndex();
    }

    @Override
    public boolean getIsCreated() {
        return isQueueCreated.get();
    }

    @Override
    public boolean getIsClosed() {
        return isQueueClosed.get();
    }

    @Override
    public long getSecondsSinceLastEnqueue() {
        return timestampOfLastEqueue.get() == 0 ? 0 :
                getCurrentTimeMillis() - timestampOfLastEqueue.get();
    }

    @Override
    public long getSecondsSinceLastConsume() {
        return timestampOfLastConsume.get() == 0 ? 0 :
                getCurrentTimeMillis() - timestampOfLastConsume.get();
    }

    @Override
    public long getSecondsSinceLastDequeue() {
        return timestampOfLastDequeue.get() == 0 ? 0 :
                getCurrentTimeMillis() - timestampOfLastDequeue.get();
    }

    public void onInit() {
        this.isQueueCreated.set(true);
        this.isQueueClosed.set(false);
    }

    public void onClose() {
        this.isQueueClosed.set(true);
        this.isQueueCreated.set(false);
    }

    public void onEnqueue(long size) {
        this.timestampOfLastEqueue.set(getCurrentTimeMillis());
        this.queueAccumulateBytes.addAndGet(size);
    }

    public void onDequeue(long size) {
        this.timestampOfLastDequeue.set(getCurrentTimeMillis());
        this.queueAccumulateBytes.addAndGet(-size);
    }

    public void onConsume() {
        this.timestampOfLastConsume.set(getCurrentTimeMillis());
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
