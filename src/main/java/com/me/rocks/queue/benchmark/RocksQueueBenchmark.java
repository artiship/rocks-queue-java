package com.me.rocks.queue.benchmark;

import com.me.rocks.queue.RocksQueue;
import com.me.rocks.queue.RocksStore;
import com.me.rocks.queue.StoreOptions;
import com.me.rocks.queue.exception.RocksQueueException;
import com.me.rocks.queue.util.Bytes;
import com.me.rocks.queue.util.Files;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.concurrent.TimeUnit.*;

@BenchmarkMode(Mode.AverageTime)
@OutputTimeUnit(NANOSECONDS)
@Warmup(iterations = 10, time = 1, timeUnit = MICROSECONDS)
@Measurement(iterations = 50, time = 1, timeUnit = MICROSECONDS)
@Fork(1)
@State(Scope.Benchmark)
public class RocksQueueBenchmark {
    private static final Logger log = LoggerFactory.getLogger(RocksQueueBenchmark.class);
    private RocksStore rocksStore;
    private RocksQueue queue;
    private byte[] bytes;
    private final String ROCKSDB_NAME = "rocks_db";
    private final String QUEUE_NAME = "queue_name";

    @Setup(Level.Trial)
    public void setUp() throws RocksQueueException {
        StoreOptions storeOptions = StoreOptions.builder()
                .database(ROCKSDB_NAME)
                .build();
        storeOptions.setDefaults();

        rocksStore = new RocksStore(storeOptions);
        queue = rocksStore.createQueue(QUEUE_NAME);
        bytes = Bytes.stringToBytes("this string is use for rocks queue benchmark testing");

        int times = 0;
        while(times < 500) {
            queue.enqueue(bytes);
            times++;
        }
        log.info("prepare data enqueue {} times", times);
    }

    @TearDown
    public void tearDown() {
        this.rocksStore.close();
        Files.deleteDirectory(ROCKSDB_NAME);
        log.info("removed the rocks db {}", ROCKSDB_NAME);
    }


    @Benchmark
    public void enqueue() throws RocksQueueException {
        queue.enqueue(bytes);
    }

    @Benchmark
    public void dequeue() throws RocksQueueException {
        queue.dequeue();
    }

    @Benchmark
    public void consume() {
        queue.consume();
    }

    @Benchmark
    public void removeHead() throws RocksDBException, RocksQueueException {
        queue.removeHead();
    }


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(RocksQueueBenchmark.class.getSimpleName()+ ".*")
                .timeUnit(MICROSECONDS)
                .forks(1)
                .build();
        new Runner(opt).run();
    }
}
