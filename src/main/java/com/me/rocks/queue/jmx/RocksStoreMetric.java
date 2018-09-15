package com.me.rocks.queue.jmx;

import com.me.rocks.queue.RocksStore;
import com.me.rocks.queue.util.Files;
import com.me.rocks.queue.util.Strings;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

public class RocksStoreMetric extends RocksMetrics implements RocksStoreMetricMXBean {
    private static long diskUsageCalculateInterval = 2 * 60 * 1000;
    private final AtomicLong lastDiskUsageCalculateTimestamp  = new AtomicLong();
    private final AtomicLong rocksDBDiskUsageInBytes = new AtomicLong();
    private final AtomicBoolean isOpen = new AtomicBoolean();
    private final AtomicBoolean isClosed = new AtomicBoolean();
    private final RocksStore rocksStore;

    public RocksStoreMetric(RocksStore rocksStore) {
        this.rocksStore = rocksStore;
    }

    @Override
    public String getDatabaseName() {
        return rocksStore.getDatabase();
    }

    @Override
    public String getRocksdbLocation() {
        return this.rocksStore.getRockdbLocation();
    }

    @Override
    public long getRocksDBDiskUsageInBytes() {
        String dbLocation = this.rocksStore.getDatabase();

        if(Strings.nullOrEmpty(dbLocation)) {
            rocksDBDiskUsageInBytes.set(0);
            return 0;
        }

        long sinceLast = getCurrentTimeMillis() - lastDiskUsageCalculateTimestamp.get();
        //Set calculate interval because of the directory size calculating has some cost
        if(sinceLast < diskUsageCalculateInterval) {
            return rocksDBDiskUsageInBytes.get();
        }

        long size = Files.getFolderSize(dbLocation);
        rocksDBDiskUsageInBytes.set(size);
        lastDiskUsageCalculateTimestamp.set(getCurrentTimeMillis());

        return size;
    }

    @Override
    public int getNumberOfQueueCreated() {
        return rocksStore.getQueueSize();
    }

    @Override
    public boolean getIsOpen() {
        return isOpen.get();
    }

    @Override
    public boolean getIsClosed() {
        return isClosed.get();
    }

    public void reset() {
        this.rocksDBDiskUsageInBytes.set(0);
        this.lastDiskUsageCalculateTimestamp.set(0);
        this.isClosed.set(false);
        this.isOpen.set(false);
    }

    public void onOpen() {
        this.isOpen.set(true);
        this.isClosed.set(false);
    }

    public void onClose() {
        this.isOpen.set(false);
        this.isClosed.set(true);
    }

    @Override
    protected String getObjectName() {
        return new StringBuilder("rocks.queue:database=")
                .append(rocksStore.getDatabase())
                .toString();
    }

    public void setDiskUsageCalculateInterval(long interval) {
        diskUsageCalculateInterval = interval;
    }
}

