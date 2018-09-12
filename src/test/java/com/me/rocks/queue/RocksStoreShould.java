package com.me.rocks.queue;

import org.junit.Test;

import static org.junit.Assert.assertNotNull;

public class RocksStoreShould {

    @Test(expected = RuntimeException.class) public void
    when_options_directory_is_empty_should_throws_exception() {
        StoreOptions options = new StoreOptions.Builder().build();
        new RocksStore(options);
    }

    @Test public void
    when_create_queue() {
        StoreOptions options = new StoreOptions.Builder().setDirectory("rocks_db").build();
        options.setDefaults();
        RocksStore rocksStore = new RocksStore(options);
        String queueName = "queue_name";
        RocksQueue queue = rocksStore.createQueue(queueName);
        assertNotNull(queue);
    }

}
