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
    when_create_store_using_options_with_empty_directory_should_throws_exception() {
        StoreOptions options = new StoreOptions.Builder().build();
        options.setDefaults();
        new RocksStore(options);
    }

    @Test public void
    when_create_a_new_queue_its_size_should_approximate_to_zero() {
        assertNotNull(queue);
        assertEquals(queue.getHeadIndex(), 0);
        assertEquals(queue.getTailIndex(),0);
        assertEquals(queue.approximateSize(), 0);
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
        rocksStore.close();
    }
}
