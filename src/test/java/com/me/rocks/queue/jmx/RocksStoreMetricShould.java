package com.me.rocks.queue.jmx;

import com.me.rocks.queue.RocksQueue;
import com.me.rocks.queue.RocksShould;
import com.me.rocks.queue.RocksStore;
import com.me.rocks.queue.StoreOptions;
import com.me.rocks.queue.exception.RocksQueueException;
import com.me.rocks.queue.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

import static org.hamcrest.Matchers.greaterThan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class RocksStoreMetricShould extends RocksShould {

    private RocksStore rocksStore;
    private String database;
    private RocksStoreMetric rocksStoreMetric;

    @Before
    public void
    init() {
        database = generateDBName();
        StoreOptions storeOptions = new StoreOptions.Builder().database(database).build();
        storeOptions.setDefaults();
        rocksStore = new RocksStore(storeOptions);
        rocksStoreMetric = rocksStore.getRocksStoreMetric();
    }

    @Test public void
    when_create_store_should_should_register_jmx() throws Exception {
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        ObjectName objectName = new ObjectName("rocks.queue:database=" + rocksStore.getDatabase());

        assertEquals(mBeanServer.getAttribute(objectName, "IsOpen"), true);
        assertEquals(mBeanServer.getAttribute(objectName, "DatabaseName"), database);
    }

    @Test public void
    when_operate_store_should_update_metrics() throws InterruptedException {
        assertEquals(rocksStoreMetric.getDatabaseName(), database);
        assertEquals(rocksStoreMetric.getRocksdbLocation(), "./" + database);
        assertThat(rocksStoreMetric.getRocksDBDiskUsageInBytes(), greaterThan(0L));
        assertEquals(rocksStoreMetric.getNumberOfQueueCreated(), 0);
        assertEquals(rocksStoreMetric.getIsOpen(), true);
        assertEquals(rocksStoreMetric.getIsClosed(), false);

        RocksQueue queue = rocksStore.createQueue(generateQueueName());
        rocksStore.createQueue(generateQueueName());
        assertEquals(rocksStoreMetric.getNumberOfQueueCreated(), 2);

        rocksStore.close();
        assertEquals(rocksStoreMetric.getIsOpen(), false);
        assertEquals(rocksStoreMetric.getIsClosed(), true);
    }


    @Test public void
    should_delay_rocks_db_disk_space_calculate_after_enqueue_dequeue() throws InterruptedException, RocksQueueException {
        RocksQueue queue = rocksStore.createQueue(generateQueueName());
        long interval = 1000;
        rocksStoreMetric.setDiskUsageCalculateInterval(interval);

        long last = rocksStoreMetric.getRocksDBDiskUsageInBytes();

        String data = "the disk usage calculating interval is 2 minutes";
        queue.enqueue(Bytes.stringToBytes(data));

        assertEquals(last, rocksStoreMetric.getRocksDBDiskUsageInBytes());

        //return the same
        Thread.sleep(interval + 100);

        assertThat(rocksStoreMetric.getRocksDBDiskUsageInBytes(), greaterThan(last));
    }

    @After public void
    finish() {
        rocksStoreMetric.unregister();
        rocksStore.close();
    }
}