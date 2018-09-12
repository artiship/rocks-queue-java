package com.me.rocks.queue;

import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class RocksQueueShould {

    private RocksQueue queue;
    private String queueName = "queue_name";

    @Before public void
    initialize() {
        StoreOptions storeOptions = new StoreOptions.Builder().setDirectory("./rocks").build();
        RocksStore rocksStore = new RocksStore(storeOptions);
        rocksStore.createQueue(queueName);
    }

    @Test public void
    should_queue_init_head_and_tail() {
        assertThat(queue.getHead(), is(1L));
        assertThat(queue.getTail(), is(0L));
    }

    @Test public void
    should_enqueue_bytes_increase_tail() {
        byte[] something = "something".getBytes();

        queue.enqueue(something);

        assertThat(queue.getHead(), is(0L));
        assertThat(queue.getTail(), is(1L));
    }

}
