package com.me.rocks.queue;

import org.junit.Test;
import org.rocksdb.CompressionType;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class StoreOptionsShould {

    @Test public void
    should_null_with_default() {
        StoreOptions options = new StoreOptions.Builder().build();
        assertNull(options.getDirectory());
    }

    @Test public void
    should_set_default() {
        StoreOptions options = new StoreOptions.Builder().build();
        options.setDefaults();

        assertEquals(options.getMemorySize(), 8 * 1024 * 1024);
        assertEquals(options.getFileSizeBase(), 64 * 1024 * 1024);
        assertEquals(options.getWriteBufferSize(), 64 * 1024 * 1024);
        assertEquals(options.getWriteBufferNumber(), 4);
        assertEquals(options.getParallel(), Math.max(Runtime.getRuntime().availableProcessors(), 2));
        assertEquals(options.getCompression(), CompressionType.LZ4HC_COMPRESSION);
        assertEquals(options.isDisableTailing(), false);
        assertEquals(options.isDisableWAL(), false);
        assertEquals(options.isWriteLogSync(), true);
    }
}