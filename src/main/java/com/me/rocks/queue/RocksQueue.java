package com.me.rocks.queue;

import com.me.rocks.queue.util.Bytes;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

public class RocksQueue {
    private static final Logger log = LoggerFactory.getLogger(RocksQueue.class);

    private static final byte[] HEAD = Bytes.stringToBytes("head");
    private static final byte[] TAIL = Bytes.stringToBytes("tail");
    private static final byte[] ONE = Bytes.longToByte(1);

    private final String queueName;

    private final AtomicLong head = new AtomicLong();
    private final AtomicLong tail = new AtomicLong();

    private final ColumnFamilyHandle cfHandle;
    private final ColumnFamilyHandle indexCfHandle;
    private final RocksIterator tailIterator;
    private final RocksStore store;

    public RocksQueue(final String queueName, final RocksStore store) {
        this.queueName = queueName;
        this.store = store;

        this.cfHandle = store.createColumnFamilyHandle(queueName);
        this.indexCfHandle = store.createColumnFamilyHandle(getIndexColumnFamilyName(queueName));

        this.tail.set(getIndexId(TAIL, 0));
        this.head.set(getIndexId(HEAD, 1));

        this.tailIterator = store.newIteratorCF(cfHandle);
    }

    private String getIndexColumnFamilyName(String queueName) {
        return new StringBuilder()
                .append("_")
                .append(queueName)
                .toString();
    }

    public boolean isEmpty() {
        return tail.get() == 0 ? true: tail.get() <= head.get();
    }

    public long getSize() {
        return tail.get() - head.get() + 1;
    }

    public long getHeadIndex() {
        return head.get();
    }

    public long getTailIndex() {
        return tail.get();
    }

    public long approximateSize() {
        return getIndexId(TAIL, 0) - getIndexId(HEAD, 1) + 1;
    }

    private long getIndexId(byte[] key, long defaultValue) {
        byte[] value = store.getCF(key, indexCfHandle);

        if(value == null) {
            return defaultValue;
        }

        return Bytes.byteToLong(value);
    }

    public long enqueue(byte[] value) {
        long id = tail.incrementAndGet();

        try(final WriteBatch writeBatch = new WriteBatch()) {
            byte[] indexId = Bytes.longToByte(id);
            writeBatch.put(cfHandle, indexId, value);
            writeBatch.merge(indexCfHandle, TAIL, ONE);
            store.write(writeBatch);
        } catch (RocksDBException e) {
            tail.decrementAndGet();
            log.error("Enqueue {} fails, {}", id, e);
            return -1;
        }

        return id;
    }

    /**
     * Polling out the head of queue
     * @return
     */
    public QueueItem dequeue() {
        QueueItem item = consume();
        removeHead();
        return item;
    }

    /**
     * Get the head of queue, in case there are deleted tombstones,
     * the final return index maybe bigger than the startId.
     * @return
     */
    public QueueItem consume() {
        if(this.getSize() == 0) {
            return null;
        }

        log.debug("Seek to head from {}", head.get());
        byte[] sid = Bytes.longToByte(head.get());
        tailIterator.seek(sid);

        if(!tailIterator.isValid()) {
            return null;
        }

        long id = Bytes.byteToLong(tailIterator.key());

        QueueItem item = new QueueItem();
        item.setKey(id);
        item.setValue(tailIterator.value());

        return item;
    }

    /**
     * remove the head from queue
     * @return
     */
    public void removeHead() {
        if(this.getSize() <= 0) {
            return;
        }

        try(final WriteBatch writeBatch = new WriteBatch()) {
            writeBatch.remove(cfHandle, Bytes.longToByte(head.get()));
            writeBatch.merge(indexCfHandle, HEAD, ONE);
            store.write(writeBatch);
            head.incrementAndGet();
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public void close() {
        cfHandle.close();
        indexCfHandle.close();
        tailIterator.close();
    }
}
