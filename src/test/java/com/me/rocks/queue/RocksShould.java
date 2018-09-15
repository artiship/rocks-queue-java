package com.me.rocks.queue;

import com.me.rocks.queue.util.Files;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class RocksShould {
    private static final Logger log = LoggerFactory.getLogger(RocksShould.class);
    private List<String> dbs = new ArrayList<>();
    private static AtomicInteger queue_index = new AtomicInteger(-1);

    protected String generateDBName() {
        String dbName = new StringBuilder()
                .append("rocks_db")
                .append("_")
                .append(dbs.size())
                .toString();

        dbs.add(dbName);

        log.info("Generate a rocks db directory {}", dbName);

        return dbName;
    }

    protected String generateQueueName() {
        String queueName = new StringBuilder()
                .append("rocks_queue")
                .append("_")
                .append(queue_index.incrementAndGet())
                .toString();

        log.info("Generate a queue name {}", queueName);

        return queueName;
    }

    protected void waitAwhileFor(long mill) {
        try {
            Thread.sleep(mill);
        } catch (InterruptedException e) {
            log.warn("Thread wait failed", e);
        }
    }


    @After
    public void tearDown() {
        for(String db: dbs) {
            Files.deleteDirectory(db);
        }
    }
}
