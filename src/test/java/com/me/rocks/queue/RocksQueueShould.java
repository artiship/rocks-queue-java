package com.me.rocks.queue;

import com.me.rocks.queue.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class RocksQueueShould extends RocksShould {
    private static final Logger log = LoggerFactory.getLogger(RocksQueueShould.class);

    private RocksStore rocksStore;
    private String queueName = "queue_name";

    @Before public void
    initialize() {
        StoreOptions storeOptions = new StoreOptions.Builder().setDirectory(generateDBName()).build();
        storeOptions.setDefaults();

        rocksStore = new RocksStore(storeOptions);
    }

    @Test public void
    should_queue_init_head_and_tail() {
        RocksQueue queue = rocksStore.createQueue(generateQueueName());

        assertThat(queue.getHead(), is(0L));
        assertThat(queue.getTail(), is(0L));
    }

    @Test public void
    should_enqueue_bytes_increase_tail() {
        RocksQueue queue = rocksStore.createQueue(generateQueueName());

        byte[] something = "something".getBytes();

        queue.enqueue(something);

        assertThat(queue.getHead(), is(0L));
        assertThat(queue.getTail(), is(1L));

        queue.close();
    }

    @Test public void
    when_enqueue_dequeue_should_forward_head_and_tail() {
        RocksQueue queue = rocksStore.createQueue(generateQueueName());

        byte[] v1 = Bytes.stringToBytes("v1");
        byte[] v2 = Bytes.stringToBytes("v2");

        queue.enqueue(v1);
        assertThat(queue.approximateSize(), is(1L));
        queue.enqueue(v2);
        assertThat(queue.approximateSize(), is(2L));

        assertEquals(queue.getHead(), 0);
        assertEquals(queue.getTail(), 2);

        QueueItem res1 = queue.dequeue();
        assertArrayEquals(v1, res1.getValue());
        assertThat(queue.approximateSize(), is(1L));

        QueueItem res2 = queue.dequeue();
        assertArrayEquals(v2, res2.getValue());
        assertThat(queue.approximateSize(), is(0L));

        assertEquals(queue.getTail(), 2);
        assertEquals(queue.getHead(), 2);

        log.info("queue tail is {} and head is {}", queue.getHead(), queue.getTail());
    }

    @Test public void
    when_consume_queue_should_return_the_head() {
        RocksQueue queue = rocksStore.createQueue(generateQueueName());

        byte[] v1 = Bytes.stringToBytes("v1");
        byte[] v2 = Bytes.stringToBytes("v2");

        long id_1 = queue.enqueue(v1);
        assertThat(queue.approximateSize(), is(1L));
        long id_2 = queue.enqueue(v2);
        assertThat(queue.approximateSize(), is(2L));

        QueueItem consume = queue.consume();
        assertEquals(consume.getKey(), id_1);
        log.info("Consumes value = {}", Bytes.bytesToString(consume.getValue()));
        assertArrayEquals(consume.getValue(), v2);

        //multiple times consumes will always return the head
        QueueItem consume2 = queue.consume();
        assertEquals(consume2.getKey(), id_1);
        assertArrayEquals(consume2.getValue(), v2);

        assertEquals(queue.getTail(), 2);
        assertEquals(queue.getHead(), 0);
        assertEquals(queue.approximateSize(), 2);
    }

    @After public void
    destroy() {
        rocksStore.close();
    }

}
