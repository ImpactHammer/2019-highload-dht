package ru.mail.polis.dao.impl;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class TimestampRecord {
    private byte[] value;
    private long timestamp;

    public static int TYPE_EMPTY = 0;
    public static int TYPE_DELETED = 1;
    public static int TYPE_VALUE = 2;

    private int type;

    public TimestampRecord(byte[] value) {
        this(value, System.currentTimeMillis());
    }

    public TimestampRecord(byte[] value, long timestamp) {
        if (value == null) {
            this.type = TYPE_DELETED;
        } else if (value.length == 0) {
            this.type = TYPE_EMPTY;
        } else {
            this.type = TYPE_VALUE;
        }
        this.value = value;
        this.timestamp = timestamp;
    }

    public byte[] getValue() {
        return this.value;
    }

    public int getType() {
        return this.type;
    }

//    public byte[] toByteArray() throws IOException {
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        ObjectOutputStream os = new ObjectOutputStream(out);
//        os.writeObject(this);
//        return out.toByteArray();
//    }

    public byte[] toByteArray() throws IOException {
        ByteBuffer buffer;
        if (value == null) {
            buffer = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
            buffer.putInt(this.type);
            buffer.putLong(this.timestamp);
        } else {
            buffer = ByteBuffer.allocate(Long.BYTES + Integer.BYTES + value.length);
            buffer.putInt(this.type);
            buffer.putLong(this.timestamp);
            buffer.put(value);
        }
        byte[] result = buffer.array();
        return result;
    }

    public static TimestampRecord fromByteArray(byte[] bytes) {
        if (bytes == null || bytes.length < Long.BYTES + Integer.BYTES) {
            return null;
        }
        ByteBuffer buffer = ByteBuffer.wrap(bytes);
        int type = buffer.getInt();
        long timestamp = buffer.getLong();
        byte[] value;
        if (type == TYPE_VALUE) {
            value = new byte[buffer.remaining()];
            buffer.get(value);
        } else if (type == TYPE_EMPTY) {
            value = new byte[0];
        } else if (type == TYPE_DELETED) {
            value = null;
        } else {
            return null;
        }
        return new TimestampRecord(value, timestamp);
    }



//    public static TimestampRecord fromByteArray(byte[] bytes) throws IOException, ClassNotFoundException {
//        if (bytes == null) {
//            return null;
//        }
//        ByteArrayInputStream in = new ByteArrayInputStream(bytes);
//        ObjectInputStream is = new ObjectInputStream(in);
//        return (TimestampRecord) is.readObject();
//    }

    public static TimestampRecord latestOf(List<TimestampRecord> records) {
        TimestampRecord result = null;
        for (TimestampRecord record : records) {
            if (record != null) {
                if (result == null) {
                    result = record;
                } else if (record.timestamp > result.timestamp) {
                    result = record;
                }
            }
        }
//        if (result != null) {
//            System.out.println(Arrays.toString(result.getValue()));
//        }
        return result;
    }
}
