package com.me.rocks.queue.jmx;

import com.me.rocks.queue.*;
import com.me.rocks.queue.exception.RocksQueueException;
import com.me.rocks.queue.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class RocksQueueMetricShould extends RocksShould {
    private static final Logger log = LoggerFactory.getLogger(RocksQueueMetricShould.class);
    private RocksStore rocksStore;
    private RocksQueue queue;
    private String queueName;
    private MBeanServer mBeanServer;
    private ObjectName objectName;
    private RocksQueueMetric rocksQueueMetric;

    private final String ENQUEUE_DATA = "ENQUEUE_DATA";
    private final long ENQUEUE_DATA_SIZE = ENQUEUE_DATA.length();
    private final long MILLSECONDS_100 = 100;

    @Before public void
    init() {
        StoreOptions storeOptions = new StoreOptions.Builder().database(generateDBName()).build();
        rocksStore = new RocksStore(storeOptions);

        queueName = this.generateQueueName();
        queue = rocksStore.createQueue(queueName);
        rocksQueueMetric = queue.getRocksQueueMetric();
    }

    @Test public void
    when_create_queue_should_should_register_jmx() throws Exception {
        mBeanServer = ManagementFactory.getPlatformMBeanServer();
        objectName = new ObjectName("rocks.queue:database="+rocksStore.getDatabase()+",queue=" + queueName);

        log.info("queue name is {}", queueName);

        assertEquals(mBeanServer.getAttribute(objectName, "QueueName"), queueName);
        assertEquals(mBeanServer.getAttribute(objectName, "IsCreated"), true);
    }

    @Test public void
    when_create_queue_should_init_metric() {
        assertEquals(rocksQueueMetric.getQueueName(), queueName);
        assertEquals(rocksQueueMetric.getQueueSize(), 0);
        assertEquals(rocksQueueMetric.getAccumulateBytes(), 0);
        assertEquals(rocksQueueMetric.getHeadIndex(), 0);
        assertEquals(rocksQueueMetric.getTailIndex(), 0);
        assertEquals(rocksQueueMetric.getIsCreated(), true);
        assertEquals(rocksQueueMetric.getIsClosed(), false);
        assertEquals(rocksQueueMetric.getSecondsSinceLastConsume(), 0);
        assertEquals(rocksQueueMetric.getSecondsSinceLastDequeue(), 0);
        assertEquals(rocksQueueMetric.getSecondsSinceLastEnqueue(), 0);
    }

    @Test public void
    when_close_queue_should_update_metric() {
        queue.close();

        assertEquals(rocksQueueMetric.getIsCreated(), false);
        assertEquals(rocksQueueMetric.getIsClosed(), true);
    }

    @Test public void
    after_operate_on_queue_should_update_metirc() throws RocksQueueException {
        queue.enqueue(Bytes.stringToBytes(ENQUEUE_DATA));

        assertEquals(rocksQueueMetric.getQueueSize(), 1);
        assertEquals(rocksQueueMetric.getAccumulateBytes(), ENQUEUE_DATA_SIZE);
        assertEquals(rocksQueueMetric.getHeadIndex(), 0);
        assertEquals(rocksQueueMetric.getTailIndex(), 1);

        waitAwhileFor(MILLSECONDS_100);

        assertThat(rocksQueueMetric.getSecondsSinceLastEnqueue(), greaterThanOrEqualTo(MILLSECONDS_100));
        log.info("seconds since last enqueue is {}", rocksQueueMetric.getSecondsSinceLastEnqueue());

        queue.consume();
        waitAwhileFor(MILLSECONDS_100);
        assertThat(rocksQueueMetric.getSecondsSinceLastConsume(), greaterThanOrEqualTo(MILLSECONDS_100));

        QueueItem dequeue = queue.dequeue();
        waitAwhileFor(MILLSECONDS_100);
        assertThat(rocksQueueMetric.getSecondsSinceLastEnqueue(), greaterThanOrEqualTo(MILLSECONDS_100));

        assertEquals(rocksQueueMetric.getQueueSize(), 0);
        assertEquals(rocksQueueMetric.getAccumulateBytes(), 0);
        assertEquals(rocksQueueMetric.getHeadIndex(), 1);
        assertEquals(rocksQueueMetric.getTailIndex(), 1);
    }


    @After public void
    finish() {
        rocksQueueMetric.unregister();
        rocksStore.close();
    }
}