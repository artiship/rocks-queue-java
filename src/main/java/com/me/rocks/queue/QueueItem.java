package com.me.rocks.queue;

import java.util.Arrays;

public class QueueItem {

    private long key;
    private byte[] value;

    public long getKey() {
        return key;
    }

    public void setKey(long key) {
        this.key = key;
    }

    public byte[] getValue() {
        return value;
    }

    public void setValue(byte[] value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return "QueueItem{" +
                "key=" + key +
                ", value=" + Arrays.toString(value) +
                '}';
    }
}
