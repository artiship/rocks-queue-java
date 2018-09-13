package com.me.rocks.queue;

import com.me.rocks.queue.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static com.me.rocks.queue.util.Bytes.byteToLong;
import static com.me.rocks.queue.util.Bytes.longToByte;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;

public class RocksDBShould extends RocksShould {

    private static final Logger logger = LoggerFactory.getLogger(RocksDBShould.class);

    private static final String QUEUE_NAME = "queue_name";
    private static final String INDEX_NAME = "_" + QUEUE_NAME;

    private final byte[] ONE = longToByte(1);
    private final byte[] HEAD = "head".getBytes();
    private final byte[] TAIL = "tail".getBytes();

    private static final String KEY = "key";
    private static final String VALUE = "value";


    static {
        RocksDB.loadLibrary();
    }

    @Before
    public void setUp() {

    }

    @Test public void
    should_put_get_and_delete_works() throws RocksDBException {
        byte[] key = "key".getBytes(UTF_8);
        byte[] value = "value".getBytes(UTF_8);

        try(final Options options = new Options().setCreateIfMissing(true)) {
            try(final RocksDB db = RocksDB.open(options, generateDBName())) {
                db.put(key, value);
                assertThat(db.get(key), is(value));
                db.delete(key);
                assertNull(db.get(key));
            }
        }
    }

    @Test public void
    should_iterate_works() throws RocksDBException {
        byte[] key = "key".getBytes(UTF_8);
        byte[] value = "value".getBytes(UTF_8);

        try(final Options options = new Options().setCreateIfMissing(true)) {
            try(final RocksDB db = RocksDB.open(options, generateDBName())) {
                db.put(key, value);

                RocksIterator it = db.newIterator();

                byte[] itValue = null;
                for(it.seek(key); it.isValid(); it.next()) {
                    logger.info("iterate key = {}", new String(it.key(), UTF_8));
                    logger.info("iterate value = {}", new String(it.value(), UTF_8));
                    itValue = it.value();
                }

                assertThat(itValue, is(value));

                db.delete(key);
            }
        }
    }

    @Test public void
    should_column_family_works() throws RocksDBException {
        try (final ColumnFamilyOptions cfOpts = new ColumnFamilyOptions()
                .optimizeUniversalStyleCompaction()
                .setMergeOperatorName("uint64add")
                .setWriteBufferSize(100);) {

            final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts)
            );

            final List<ColumnFamilyHandle> columnFamilyHandleList =
                    new ArrayList<>();

            try (final DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
                 final RocksDB db = RocksDB.open(options, generateDBName(), cfDescriptors, columnFamilyHandleList);
                 WriteBatch writeBatch = new WriteBatch()) {
                try {
                    ColumnFamilyHandle columnFamily = db.createColumnFamily(new ColumnFamilyDescriptor(QUEUE_NAME.getBytes(), cfOpts));
                    ColumnFamilyHandle indexColumnFamily = db.createColumnFamily(new ColumnFamilyDescriptor(INDEX_NAME.getBytes(), cfOpts));

                    writeBatch.put(columnFamily, KEY.getBytes(), VALUE.getBytes());
                    writeBatch.merge(indexColumnFamily, HEAD, ONE);
                    writeBatch.merge(indexColumnFamily, TAIL, ONE);
                    writeBatch.merge(indexColumnFamily, HEAD, ONE);
                    writeBatch.merge(indexColumnFamily, TAIL, ONE);
                    writeBatch.merge(indexColumnFamily, TAIL, ONE);

                    WriteOptions writeOptions = new WriteOptions();
                    db.write(writeOptions, writeBatch);

                    long head = byteToLong(db.get(indexColumnFamily, HEAD));
                    long tail = byteToLong(db.get(indexColumnFamily, TAIL));
                    String value = Bytes.bytesToString(db.get(columnFamily, KEY.getBytes()));

                    assertEquals(head, 2);
                    assertEquals(tail, 3);
                    assertEquals(value, VALUE);

                    logger.info("queue head is {}", head);
                    logger.info("queue tail is {}", tail);
                    logger.info("get {}={} from rocks db", KEY, VALUE);

                    writeOptions.close();
                    columnFamily.close();
                    indexColumnFamily.close();
                } finally {
                    for (final ColumnFamilyHandle handle : columnFamilyHandleList) {
                        handle.close();
                    }
                } // frees the db and the db options
            }// frees the column family options
            catch (RocksDBException e) {
                e.printStackTrace();
            }
        }
    }
}
