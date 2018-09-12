package com.me.rocks.queue;

import com.me.rocks.queue.util.ByteConversionHelper;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class RocksQueue {
    private static final Logger log = LoggerFactory.getLogger(RocksQueue.class);
    private static final byte[] HEAD = ByteConversionHelper.stringToBytes("head");
    private static final byte[] TAIL = ByteConversionHelper.stringToBytes("tail");
    private static final byte[] ONE = ByteConversionHelper.longToByte(1);

    private String queueName;
    private AtomicLong head = new AtomicLong();
    private AtomicLong tail = new AtomicLong();
    private boolean useTailing;
    private AtomicLong id = new AtomicLong();

    private ColumnFamilyHandle cfHandle;
    private ColumnFamilyHandle indexCfHandle;
    private RocksIterator tailIterator;
    private RocksStore store;

    public RocksQueue(String queueName, RocksStore store, boolean useTailing) {
        this.queueName = queueName;
        this.useTailing = useTailing;

        this.cfHandle = store.createColumnFamilyHandle(queueName);
        this.indexCfHandle = store.createColumnFamilyHandle(getIndexColumnFamilyName(queueName));

        this.head.set(getHead());
        this.tail.set(getTail());

        if (useTailing) {
            this.store = store;
            this.tailIterator = store.newIteratorCF(cfHandle);
        }
    }

    private String getIndexColumnFamilyName(String queueName) {
        return new StringBuilder()
                .append("_")
                .append(queueName)
                .toString();
    }

    public long approximateSize() {
        return getTail() - getHead() + 1;
    }

    public long getHead() {
        return getIndexId(HEAD, 1);
    }

    public long getTail() {
        return getIndexId(TAIL, 0);
    }

    private long getIndexId(byte[] key, int defaultValue) {
        long index = defaultValue;
        try {
            byte[] value = store.getCF(key, indexCfHandle);
            index = ByteConversionHelper.byteToLong(value);
        } catch (Exception e) {
            log.error("Failed to get {} from rocksdb, {}", key, e);
        }
        return index;
    }

    public long enqueue(byte[] value) {
        long id = tail.incrementAndGet();

        try(final WriteBatch writeBatch = new WriteBatch()) {
            byte[] key = ByteConversionHelper.longToByte(id);
            writeBatch.put(cfHandle, key, value);
            writeBatch.merge(indexCfHandle, TAIL, ONE);
            store.write(writeBatch);
        }

        return id;
    }

    public Map<Long, byte[]> dequeue(long startId) {

        Map<Long, byte[]> result = new HashMap<>();

        long seekId = 1;
        if (startId > 0) {
            seekId = startId;
        }

        RocksIterator it = null;

        byte[] sid = ByteConversionHelper.longToByte(seekId);
        if(useTailing) {
            it = tailIterator;
            if(!it.isValid()) {
                it.seek(sid);
            }
        } else {
            it = store.newIteratorCF(cfHandle);
            it.seek(sid);
        }

        if(!it.isValid()) {
            return null;
        }

        try(final WriteBatch writeBatch = new WriteBatch()) {
            writeBatch.remove(cfHandle, it.key());
            writeBatch.merge(indexCfHandle, HEAD, ONE);
            store.write(writeBatch);

            head.incrementAndGet();
            if (useTailing) {
                it.next();
            }
        }

        long id = ByteConversionHelper.byteToLong(it.key());
        result.put(id, it.value());

        log.debug("[Queue={}] Dequeued data {}", queueName, result);

        return result;
    }
}
