package com.me.rocks.queue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class RocksStoreShould extends RocksShould {

    private StoreOptions options;
    private RocksStore rocksStore;
    private RocksQueue queue;

    @Before public void
    setUp() {
        options = new StoreOptions.Builder().setDirectory(generateDBName()).build();
        options.setDefaults();
        rocksStore = new RocksStore(options);
        queue = rocksStore.createQueue(generateQueueName());
    }

    @Test(expected = RuntimeException.class) public void
    when_options_directory_is_empty_should_throws_exception() {
        StoreOptions options = new StoreOptions.Builder().build();
        options.setDefaults();
        new RocksStore(options);
    }

    @Test public void
    should_newly_created_queue_size_approximate_to_zero() {
        assertNotNull(queue);
        assertThat(queue.getHead(), is(0L));
        assertThat(queue.getTail(), is(0L));
        assertThat(queue.approximateSize(), is(0L));
    }

    @Test public void
    when_create_queue_by_the_same_name_should_always_return_the_same_queue() {
        RocksQueue q1 = rocksStore.createQueue(generateQueueName());
        RocksQueue q2 = rocksStore.createQueue(generateQueueName());

        assertEquals(queue, q1);
        assertEquals(q1, q2);
    }

    @After public void
    destroy() {
        queue.close();
        rocksStore.close();
    }
}
