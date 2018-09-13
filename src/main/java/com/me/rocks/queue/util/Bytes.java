package com.me.rocks.queue.util;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;

public class Bytes {
    /**
     * Transforms a long value to the platform-specific
     * byte representation.
     *
     * @param value numeric value.
     * @return platform-specific long value.
     */
    public static byte[] longToByte(long value) {
        ByteBuffer longBuffer = ByteBuffer.allocate(8)
                .order(ByteOrder.nativeOrder());
        longBuffer.clear();
        longBuffer.putLong(value);
        return longBuffer.array();
    }

    /**
     * Transforms platform specific byte representation
     * to long value.
     *
     * @param data platform-specific byte array.
     *
     * @return numeric value.
     */
    public static long byteToLong(byte[] data) {
        if (data != null) {
            ByteBuffer longBuffer = ByteBuffer.allocate(8)
                    .order(ByteOrder.nativeOrder());
            longBuffer.put(data, 0, 8);
            longBuffer.flip();
            return longBuffer.getLong();
        }
        return 0;
    }

    public static byte[] stringToBytes(String key) {
        if(key == null) {
            return null;
        }
        return key.getBytes(StandardCharsets.UTF_8);
    }

    public static String bytesToString(byte[] bytes) {
        if(bytes == null) {
            return null;
        }
        return new String(bytes, StandardCharsets.UTF_8);
    }

    /**
     * Utility constructor
     */
    private Bytes() {
    }
}
