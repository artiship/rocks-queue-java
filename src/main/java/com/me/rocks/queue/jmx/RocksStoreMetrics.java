package com.me.rocks.queue.jmx;

import com.me.rocks.queue.RocksStore;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class RocksStoreMetrics {
    private final AtomicReference<String> rocksdbLocation= new AtomicReference<String>();
    private final AtomicLong rocksDBDiskUsageInBytes = new AtomicLong();
    private final Map<String, RocksStoreMetrics> queues = new ConcurrentHashMap<>();
    private final AtomicLong queueSize = new AtomicLong();
    private final AtomicBoolean isOpen = new AtomicBoolean();
    private final AtomicBoolean isClosed = new AtomicBoolean();

    public RocksStoreMetrics(RocksStore rocksStore) {
        rocksStore.registerStatisticsListener(this);
    }

    public void reset() {
        this.rocksdbLocation.set("");
        this.rocksDBDiskUsageInBytes.set(0);
        this.queues.clear();
        this.queueSize.set(0);
        this.isClosed.set(false);
        this.isOpen.set(false);
    }

    public AtomicReference<String> getRocksdbLocation() {
        return rocksdbLocation;
    }

    public AtomicLong getRocksDBDiskUsageInBytes() {
        return rocksDBDiskUsageInBytes;
    }

    public Map<String, RocksStoreMetrics> getQueues() {
        return queues;
    }

    public AtomicLong getQueueSize() {
        return queueSize;
    }

    public AtomicBoolean getIsOpen() {
        return isOpen;
    }

    public AtomicBoolean getIsClosed() {
        return isClosed;
    }
}

