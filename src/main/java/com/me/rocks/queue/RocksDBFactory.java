package com.me.rocks.queue;

import com.me.rocks.queue.util.ByteConversionHelper;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

final public class RocksDBFactory {
    private static RocksDBStore store;

    private RocksDBFactory() {

    }

    static class RocksDBStore {
        private Options options;
        private RocksDB db;
        private final ColumnFamilyOptions cfOpts;
        private final List<ColumnFamilyDescriptor> cfDescriptors;
        private final List<ColumnFamilyHandle> columnFamilyHandleList;
        private String rocksDBPath = "./";

        private static final Logger log = LoggerFactory.getLogger(RocksQueue.class);


        static {
            RocksDB.loadLibrary();
        }
        private static final byte[] byteArrayOne = ByteConversionHelper.longToByte(1);

        public RocksDBStore(final String name, final StoreOptions opts) {
            cfOpts = new ColumnFamilyOptions()
                    .optimizeUniversalStyleCompaction()
                    .setMergeOperatorName("uint64add")
                    .setWriteBufferSize(100);

            cfDescriptors = Arrays.asList(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
                    new ColumnFamilyDescriptor("head".getBytes(), cfOpts),
                    new ColumnFamilyDescriptor("tail".getBytes(), cfOpts)
            );

            columnFamilyHandleList = new ArrayList<>();

            final DBOptions options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
            try {
                db = RocksDB.open(options, getPath(name), cfDescriptors, columnFamilyHandleList);
            } catch (RocksDBException e) {
                log.error("Open rocksdb failed, {}", e);
                close();
            }
        }

        public void close() {
            cfOpts.close();
            for(ColumnFamilyHandle handle: columnFamilyHandleList) {
                handle.close();
            }
            options.close();
            db.close();
        }

        private String getPath(String name) {
            return rocksDBPath + "/" + name;
        }

        public long get(String name) {
            return 1;
        }
    }

    public static RocksDBStore getInstance(final String name, final StoreOptions options) {
        if(store == null)
            store = new RocksDBStore(name, options);

        return store;
    }
}
