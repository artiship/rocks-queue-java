package com.me.rocks.queue;

import com.me.rocks.queue.jmx.RocksStoreMetric;
import com.me.rocks.queue.util.Bytes;
import com.me.rocks.queue.util.Files;
import com.me.rocks.queue.util.Strings;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

public class RocksStore {
    private static final Logger log = LoggerFactory.getLogger(RocksStore.class);
    private final String database;
    private final String directory;
    private final HashMap<String, RocksQueue> queues;
    private final DBOptions dbOptions;
    private final ColumnFamilyOptions cfOpts;
    private final ArrayList<ColumnFamilyHandle> cfHandles;
    private final ReadOptions readOptions;
    private final WriteOptions writeOptions;
    private final RocksDB db;
    private final RocksStoreMetric rocksStoreMetric;
    private final Map<String, ColumnFamilyHandle> columnFamilyHandleMap = new HashMap<>();

    static {
        RocksDB.loadLibrary();
    }

    public RocksStore(StoreOptions options) {
        if(Strings.nullOrEmpty(options.getDatabase())) {
            throw new RuntimeException("Empty database of store options");
        }

        if(options.isDebug()) {
            log.isDebugEnabled();
        }

        this.directory = options.getDirectory();
        this.database = options.getDatabase();
        this.cfHandles = new ArrayList<ColumnFamilyHandle>();
        this.queues = new HashMap<String, RocksQueue>();

        this.readOptions = new ReadOptions()
                .setFillCache(false)
                .setTailing(!options.isDisableTailing());
        this.writeOptions = new WriteOptions()
                .setDisableWAL(options.isDisableWAL())
                .setSync(options.isWriteLogSync());

        Files.mkdirIfNotExists(this.getRockdbLocation());

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

        this.rocksStoreMetric = new RocksStoreMetric(this);
        this.rocksStoreMetric.register();

        db = openRocksDB();
    }

    private RocksDB openRocksDB() {
        RocksDB rocksDB;

        final List<ColumnFamilyDescriptor> cfDescriptors = new ArrayList<>();
        final List<ColumnFamilyHandle> columnFamilyHandleList = new ArrayList<>();

        //load existing column families
        try {
            List<byte[]> columnFamilies = RocksDB.listColumnFamilies(new Options(), this.database);
            log.debug("Load existing column families {}", columnFamilies.stream().map(cf -> Bytes.bytesToString(cf)).collect(toList()));

            columnFamilies.forEach( cf -> cfDescriptors.add(new ColumnFamilyDescriptor(cf, cfOpts)));
        } catch (RocksDBException e) {
            log.warn("Load existing column families failed.", e);
        }

        if(cfDescriptors.isEmpty()) {
            cfDescriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfOpts));
        }

        try {
            rocksDB = RocksDB.open(dbOptions, database, cfDescriptors, columnFamilyHandleList);
        } catch (RocksDBException e) {
            log.error("Failed to open rocks database, try to remove rocks db database {}", database, e);
            Files.deleteDirectory(database);
            try {
                rocksDB = RocksDB.open(dbOptions, database, cfDescriptors, columnFamilyHandleList);
                log.info("Recreate rocks db at {} again from scratch", database);
            } catch (RocksDBException e1) {
                log.error("Failed to create rocks db again at {}", database, e);
                throw new RuntimeException("Failed to create rocks db again.");
            }
        }

        this.rocksStoreMetric.onOpen();

        //Cache <columnFamilyName, columnFamilyHandle> relations
        for (int i = 0; i < cfDescriptors.size(); i++) {
            ColumnFamilyDescriptor columnFamilyDescriptor = cfDescriptors.get(i);
            if(columnFamilyDescriptor != null) {
                columnFamilyHandleMap.put(Bytes.bytesToString(columnFamilyDescriptor.columnFamilyName()),
                        columnFamilyHandleList.get(i));
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

        this.rocksStoreMetric.onClose();
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

        if(columnFamilyHandleMap.containsKey(cfName)) {
            return columnFamilyHandleMap.get(cfName);
        }

        final ColumnFamilyDescriptor cfDescriptor = new ColumnFamilyDescriptor(Bytes.stringToBytes(cfName),
                cfOpts);

        ColumnFamilyHandle handle = null;
        try {
            handle = db.createColumnFamily(cfDescriptor);
        } catch (RocksDBException e) {
            log.error("Create column family fail", e);
        }

        columnFamilyHandleMap.put(cfName, handle);

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

    public int getQueueSize() {
        return queues.size();
    }

    public String getDatabase() {
        return this.database;
    }

    public String getRockdbLocation() {
        if(Strings.nullOrEmpty(directory)) {
            return "./" + database;
        }

        return directory + File.pathSeparator + database;
    }

    public RocksStoreMetric getRocksStoreMetric() {
        return this.rocksStoreMetric;
    }
}
