package com.me.rocks.queue;

import com.me.rocks.queue.exception.RocksQueueException;
import com.me.rocks.queue.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.LongStream;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;

public class RocksQueueShould extends RocksShould {
    private static final Logger log = LoggerFactory.getLogger(RocksQueueShould.class);

    private RocksStore rocksStore;
    private RocksQueue queue;

    @Before public void
    initialize() {
        StoreOptions storeOptions = new StoreOptions.Builder().database(generateDBName()).build();
        storeOptions.setDefaults();

        rocksStore = new RocksStore(storeOptions);
        queue = rocksStore.createQueue(generateQueueName());
    }

    @Test public void
    when_create_a_new_queue_should_init_head_and_tail() {
        assertThat(queue.getTailIndex(), is(0L));
        assertThat(queue.getHeadIndex(), is(0L));
        assertTrue(queue.isEmpty());
        assertThat(queue.approximateSize(), is(0L));
    }

    @Test public void
    when_enqueue_bytes_should_increase_tail() throws RocksQueueException {
        byte[] something = "something".getBytes();

        queue.enqueue(something);

        assertThat(queue.getHeadIndex(), is(0L));
        assertThat(queue.getTailIndex(), is(1L));
    }

    @Test public void
    when_enqueue_dequeue_should_forward_head_and_tail() throws RocksQueueException {
        byte[] v1 = Bytes.stringToBytes("v1");
        byte[] v2 = Bytes.stringToBytes("v2");

        long id_1 = queue.enqueue(v1);
        assertEquals(queue.approximateSize(), 1);
        assertEquals(id_1, 1);
        long id_2 = queue.enqueue(v2);
        assertEquals(queue.approximateSize(), 2);
        assertEquals(id_2, 2);

        assertEquals(queue.getHeadIndex(), 0);
        assertEquals(queue.getTailIndex(), 2);

        QueueItem res1 = queue.dequeue();
        assertArrayEquals(res1.getValue(), v1);
        assertEquals(queue.getHeadIndex(), 1);
        assertEquals(queue.approximateSize(), 1);

        QueueItem res2 = queue.dequeue();
        assertArrayEquals(res2.getValue(), v2);
        assertThat(queue.approximateSize(), is(0L));
        assertThat(queue.getSize(), is(0L));

        assertEquals(queue.getTailIndex(), 2);
        assertEquals(queue.getHeadIndex(), 2);

        log.info("queue tail is {} and head is {}", queue.getHeadIndex(), queue.getTailIndex());
    }

    @Test public void
    when_consume_queue_should_return_the_head() throws RocksQueueException {
        byte[] v1 = Bytes.stringToBytes("v1");
        byte[] v2 = Bytes.stringToBytes("v2");

        long id_1 = queue.enqueue(v1);
        assertThat(queue.approximateSize(), is(1L));
        long id_2 = queue.enqueue(v2);
        assertThat(queue.approximateSize(), is(2L));

        QueueItem consume = queue.consume();
        assertEquals(consume.getIndex(), id_1);
        log.info("Consumes value = {}", Bytes.bytesToString(consume.getValue()));
        assertArrayEquals(consume.getValue(), v1);

        //multiple times consumes will always return the head
        QueueItem consume2 = queue.consume();
        assertEquals(consume2.getIndex(), id_1);
        assertArrayEquals(consume2.getValue(), v1);

        assertEquals(queue.getTailIndex(), 2);
        assertEquals(queue.getHeadIndex(), 0);
        assertEquals(queue.approximateSize(), 2);
    }

    @Test public void
    when_queue_is_empty_dequeue_should_return_null() throws RocksQueueException {
        assertEquals(queue.getSize(), 0);
        assertNull(queue.dequeue());
    }

    @Test public void
    when_queue_is_empty_consume_should_return_null() {
        assertEquals(queue.getSize(), 0);
        assertNull(queue.consume());
    }

    @Test public void
    should_dequeue_as_the_order_enqueue() throws RocksQueueException {
        List<Long> enqueueList = new ArrayList<>();
        List<Long> dequeueList = new ArrayList<>();

        LongStream.iterate(1, i -> i + 1)
                .limit(10)
                .forEach(i -> {
                    enqueueList.add(i);
                    try {
                        queue.enqueue(Bytes.longToByte(i));
                    } catch (RocksQueueException e) {
                        log.error("Initialize data error", e);
                    }
                });

        while(queue.getSize() > 0) {
            QueueItem dequeue = queue.dequeue();
            long val = Bytes.byteToLong(dequeue.getValue());
            dequeueList.add(val);
        }

        log.info("enqueue list {}", enqueueList);
        log.info("dequeue list {}", dequeueList);

        assertArrayEquals(enqueueList.toArray(), dequeueList.toArray());
    }

    @After public void
    destroy() {
        rocksStore.close();
    }

}
