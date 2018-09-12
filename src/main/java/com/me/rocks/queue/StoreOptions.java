package com.me.rocks.queue;

import org.rocksdb.CompressionType;

public class StoreOptions {
    private String directory;
    private int writeBufferSize;
    private int writeBufferNumber;
    private int memorySize;
    private long fileSizeBase;
    private CompressionType compression;
    private int parallel;
    private boolean disableAutoCompaction;
    private boolean disableWAL;
    private boolean disableTailing;
    private boolean writeLogSync;
    private boolean isDebug;

    private StoreOptions(Builder builder) {
        this.directory = builder.directory;
        this.writeBufferSize = builder.writeBufferSize;
        this.writeBufferNumber = builder.writeBufferNumber;
        this.memorySize = builder.memorySize;
        this.fileSizeBase = builder.fileSizeBase;
        this.compression = builder.compression;
        this.parallel = builder.parallel;
        this.disableAutoCompaction = builder.disableAutoCompaction;
        this.disableWAL = builder.disableWAL;
        this.disableTailing = builder.disableTailing;
        this.writeLogSync = builder.writeLogSync;
        this.isDebug = builder.isDebug;
    }

    public void setDefaults() {
        if (this.memorySize <= 0) this.memorySize = 8 * 1024 * 1024;
        if (this.fileSizeBase <= 0) this.fileSizeBase = 64 * 1024 * 1024;
        if (this.writeBufferSize <= 0) this.writeBufferSize = 64 * 1024 * 1024;
        if (this.writeBufferNumber <= 0) this.writeBufferNumber = 4;
        if (this.parallel <= 0) this.parallel = Math.max(Runtime.getRuntime().availableProcessors(), 2);
        if (this.compression == null) this.compression = CompressionType.LZ4HC_COMPRESSION;

        this.disableTailing = false;
        this.disableWAL = false;
        this.writeLogSync = true;
    }

    public String getDirectory() {
        return directory;
    }

    public int getWriteBufferSize() {
        return writeBufferSize;
    }

    public int getWriteBufferNumber() {
        return writeBufferNumber;
    }

    public int getMemorySize() {
        return memorySize;
    }

    public long getFileSizeBase() {
        return fileSizeBase;
    }

    public CompressionType getCompression() {
        return compression;
    }

    public int getParallel() {
        return parallel;
    }

    public boolean isDisableAutoCompaction() {
        return disableAutoCompaction;
    }

    public boolean isDisableWAL() {
        return disableWAL;
    }

    public boolean isDisableTailing() {
        return disableTailing;
    }

    public boolean isWriteLogSync() {
        return writeLogSync;
    }

    public boolean isDebug() {
        return isDebug;
    }

    public static class Builder {
        private String directory;
        private int writeBufferSize;
        private int writeBufferNumber;
        private int memorySize;
        private long fileSizeBase;
        private CompressionType compression;
        private int parallel;
        private boolean disableAutoCompaction;
        private boolean disableWAL;
        private boolean disableTailing;
        private boolean writeLogSync;
        private boolean isDebug;

        public StoreOptions build() {
            return new StoreOptions(this);
        }

        public Builder setDirectory(String directory) {
            this.directory = directory;
            return this;
        }

        public Builder setWriteBufferSize(int writeBufferSize) {
            this.writeBufferSize = writeBufferSize;
            return this;
        }

        public Builder setWriteBufferNumber(int writeBufferNumber) {
            this.writeBufferNumber = writeBufferNumber;
            return this;
        }

        public Builder setMemorySize(int memorySize) {
            this.memorySize = memorySize;
            return this;
        }

        public Builder setFileSizeBase(long fileSizeBase) {
            this.fileSizeBase = fileSizeBase;
            return this;
        }

        public Builder setCompression(CompressionType compression) {
            this.compression = compression;
            return this;
        }

        public Builder setParallel(int parallel) {
            this.parallel = parallel;
            return this;
        }

        public Builder setDisableAutoCompaction(boolean disableAutoCompaction) {
            this.disableAutoCompaction = disableAutoCompaction;
            return this;
        }

        public Builder setDisableWAL(boolean disableWAL) {
            this.disableWAL = disableWAL;
            return this;
        }

        public Builder setDisableTailing(boolean disableTailing) {
            this.disableTailing = disableTailing;
            return this;
        }

        public Builder setWriteLogSync(boolean writeLogSync) {
            this.writeLogSync = writeLogSync;
            return this;
        }
        public Builder setDebug(boolean debug) {
            isDebug = debug;
            return this;
        }
    }
}
