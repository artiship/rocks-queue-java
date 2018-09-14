package com.me.rocks.queue;

import java.util.Arrays;

public class QueueItem {

    private long index;
    private byte[] value;

    public QueueItem(long index, byte[] value) {
        this.index = index;
        this.value = value;
    }

    public long getIndex() {
        return index;
    }

    public void setIndex(long index) {
        this.index = index;
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
                "index=" + index +
                ", value=" + Arrays.toString(value) +
                '}';
    }
}
