package com.me.rocks.queue;

import com.me.rocks.queue.util.Bytes;
import com.me.rocks.queue.util.Files;
import com.me.rocks.queue.util.Strings;
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
    private final RocksDB db;

    static {
        RocksDB.loadLibrary();
    }

    public RocksStore(StoreOptions options) {
        if(Strings.nullOrEmpty(options.getDirectory())) {
            throw new RuntimeException("Empty directory of store options");
        }

        if(options.isDebug()) {
            log.isDebugEnabled();
        }

        this.directory = options.getDirectory();
        this.useTailing = !options.isDisableTailing();
        this.cfHandles = new ArrayList<ColumnFamilyHandle>();
        this.queues = new HashMap<String, RocksQueue>();

        this.readOptions = new ReadOptions()
                .setFillCache(false)
                .setTailing(!options.isDisableTailing());
        this.writeOptions = new WriteOptions()
                .setDisableWAL(options.isDisableWAL())
                .setSync(options.isWriteLogSync());

        Files.mkdirIfNotExists(directory);

        this.dbOptions = new DBOptions()
                .setCreateIfMissing(true)
                .setIncreaseParallelism(options.getParallel())
                .setCreateMissingColumnFamilies(true)
                .setMaxOpenFiles(-1);

        final BlockBasedTableConfig blockBasedTableConfig = new BlockBasedTableConfig()
                .setBlockCacheSize(options.getMemorySize())
                .setFilter(new BloomFilter(10));

        this.cfOpts = new ColumnFamilyOptions()
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
                .setTableFormatConfig(blockBasedTableConfig)
                .setMemtablePrefixBloomSizeRatio(0.1);

        if(options.getCompression() != null) {
            cfOpts.setCompressionType(options.getCompression());
        }

        db = openRocksDB();
    }

    private RocksDB openRocksDB() {
        RocksDB rocksDB;

        final List<ColumnFamilyHandle> columnFamilyHandleList =
                new ArrayList<>();

        final List<ColumnFamilyDescriptor> cfDescriptors = Arrays.asList(
                new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts)
        );

        try {
            rocksDB = RocksDB.open(dbOptions, directory, cfDescriptors, columnFamilyHandleList);
        } catch (RocksDBException e) {
            log.error("Failed to open rocks database on {}, {}", directory, e);
            log.info("Try to remove rocks db directory {}", directory);
            Files.deleteDirectory(directory);
            try {
                log.info("Try to create rocks db at {} again from scratch", directory);
                rocksDB = RocksDB.open(dbOptions, directory, cfDescriptors, columnFamilyHandleList);
            } catch (RocksDBException e1) {
                log.error("Failed to create rocks db again at {}, {}", directory, e);
                throw new RuntimeException("Failed to create rocks db again.");
            }
        }

        return rocksDB;
    }

    public void close() {
        readOptions.close();
        writeOptions.close();

        dbOptions.close();
        cfOpts.close();
        for(ColumnFamilyHandle handle: cfHandles) {
            handle.close();
        }

        for (RocksQueue rocksQueue: queues.values()) {
            if(rocksQueue != null) {
                rocksQueue.close();
            }
        }

        db.close();
    }

    public RocksQueue createQueue(final String queueName) {
        if(Strings.nullOrEmpty(queueName)){
            throw new IllegalArgumentException("Create rocks queue name can't not be null or empty");
        }

        if(queues.containsKey(queueName)) {
            return queues.get(queueName);
        }

        RocksQueue queue = new RocksQueue(queueName, this);

        queues.put(queueName, queue);

        return queue;
    }

    public ColumnFamilyHandle createColumnFamilyHandle(String cfName) {
        if(Strings.nullOrEmpty(cfName)) {
            return null;
        }

        ColumnFamilyHandle handle = null;
        final ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(Bytes.stringToBytes(cfName),
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

    public void write(WriteBatch writeBatch) throws RocksDBException {
        db.write(this.writeOptions, writeBatch);
    }
}
