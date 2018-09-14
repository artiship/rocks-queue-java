package com.me.rocks.queue.jmx;

import com.me.rocks.queue.RocksQueue;
import com.me.rocks.queue.RocksShould;
import com.me.rocks.queue.RocksStore;
import com.me.rocks.queue.StoreOptions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.*;
import java.lang.management.ManagementFactory;

import static org.junit.Assert.assertEquals;

public class RocksQueueMetricsShould extends RocksShould {

    private RocksStore rocksStore;
    private RocksQueue queue;

    @Before public void
    init(){
        StoreOptions storeOptions = new StoreOptions.Builder().setDirectory(generateDBName()).build();
        storeOptions.setDefaults();

        rocksStore = new RocksStore(storeOptions);
        queue = rocksStore.createQueue(generateQueueName());
    }

    @Test public void
    jmx_registration() throws Exception {
        MBeanServer mBeanServer = ManagementFactory.getPlatformMBeanServer();
        RocksQueueMetrics rocksQueueStatistics = new RocksQueueMetrics(queue);

        ObjectName objectName = new ObjectName("rocks.queue:type=RocksQueueMetrics");
        mBeanServer.registerMBean(rocksQueueStatistics, objectName);
        try {
            assertEquals(mBeanServer.getAttribute(objectName, "Connected"), Boolean.FALSE);
        } finally {
            mBeanServer.unregisterMBean(objectName);
        }
    }

    @After public void
    finish() {
        rocksStore.close();
    }
}