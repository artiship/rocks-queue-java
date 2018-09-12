package com.me.rocks.queue;

import com.me.rocks.queue.util.ByteConversionHelper;
import com.me.rocks.queue.util.Utils;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class RocksStore {
    private static final Logger log = LoggerFactory.getLogger(RocksStore.class);
    private final String directory;
    private final boolean useTailing;
    private final HashMap<String, RocksQueue> queues;
    private final DBOptions dbOptions;
    private final ColumnFamilyOptions cfOpts;
    private final ArrayList<ColumnFamilyHandle> cfHandles;
    private final ReadOptions readOptions;
    private final WriteOptions writeOptions;
    private RocksDB db;


    static {
        RocksDB.loadLibrary();
    }

    public RocksStore(StoreOptions options) {
        if(Utils.nullOrEmpty(options.getDirectory())) {
            throw new RuntimeException("Empty directory of store options");
        }

        if(options.isDebug()) {
            log.isDebugEnabled();
        }

        this.directory = options.getDirectory();
        this.useTailing = !options.isDisableTailing();
        this.cfHandles = new ArrayList<ColumnFamilyHandle>();
        this.queues = new HashMap<String, RocksQueue>();

        Utils.mkdirIfNotExists(directory);

        dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setIncreaseParallelism(options.getParallel())
                .setCreateMissingColumnFamilies(true)
                .setMaxOpenFiles(-1);

        final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig()
                .setBlockCacheSize(options.getMemorySize())
                .setFilter(new BloomFilter(10));

        cfOpts = new ColumnFamilyOptions()
                .optimizeUniversalStyleCompaction()
                .setMergeOperatorName("uint64add")
                .setMaxSuccessiveMerges(64)
                .setWriteBufferSize(options.getWriteBufferSize())
                .setTargetFileSizeBase(options.getFileSizeBase())
                .setLevel0FileNumCompactionTrigger(8)
                .setLevel0SlowdownWritesTrigger(16)
                .setLevel0StopWritesTrigger(24)
                .setNumLevels(4)
                .setMaxBytesForLevelBase(512 * 1024 * 1024)
                .setMaxBytesForLevelMultiplier(8)
                .setCompressionType(options.getCompression())
                .setTableFormatConfig(blockBasedTableConfig)
                .setMemtablePrefixBloomSizeRatio(0.1);

        final List<ColumnFamilyHandle> columnFamilyHandleList =
                new ArrayList<>();

        /*List<byte[]> cfNames = null;
        try {
            cfNames = RocksDB.listColumnFamilies(, this.directory);
        } catch (RocksDBException e) {
            log.error("Failed to collect the column family names, {}", e);
        }

        log.debug("Got column names for the existing db, ", cfNames);*/

        final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts),
                new ColumnFamilyDescriptor("head".getBytes(), cfOpts),
                new ColumnFamilyDescriptor("tail".getBytes(), cfOpts)
        );

        try {
            db = RocksDB.open(dbOptions, directory, cfDescriptors, columnFamilyHandleList);
        } catch (RocksDBException e) {
            log.error("failed to open rocks database, {}", e);
        }

        readOptions = new ReadOptions()
                .setFillCache(false)
                .setTailing(!options.isDisableTailing());
        writeOptions = new WriteOptions()
                .setDisableWAL(options.isDisableWAL())
                .setSync(options.isWriteLogSync());
    }

    public void close() {
        readOptions.close();
        writeOptions.close();

        dbOptions.close();
        cfOpts.close();
        for(ColumnFamilyHandle handle: cfHandles) {
            handle.close();
        }

        db.close();
    }

    public RocksQueue createQueue(String queueName) {
        ColumnFamilyHandle cfHandle = cfHandles.get(0);

        return new RocksQueue(queueName, this, useTailing);
    }

    public ColumnFamilyHandle createColumnFamilyHandle(String cfName) {
        if(Utils.nullOrEmpty(cfName)) {
            return null;
        }

        ColumnFamilyHandle handle = null;
        ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(ByteConversionHelper.stringToBytes(cfName),
                cfOpts);
        try {
            handle = db.createColumnFamily(cfDescriptor);
        } catch (RocksDBException e) {
            log.error("Create column family ");
        }

        return handle;
    }

    public RocksIterator newIteratorCF(ColumnFamilyHandle cfHandle) {
        return db.newIterator(cfHandle, this.readOptions);
    }

    public byte[] getCF(byte[] key, ColumnFamilyHandle cfHandle) {
        byte[] value = null;
        try {
            value = db.get(cfHandle, key);
        } catch (RocksDBException e) {
            log.error("Failed to get {} from rocks db, {}", key, e);
        }
        return value;
    }

    public void write(WriteBatch writeBatch) {
        try {
            db.write(this.writeOptions, writeBatch);
        } catch (RocksDBException e) {
            log.error("Write batch into rocks db fails, {}", e);
        }
    }
}
