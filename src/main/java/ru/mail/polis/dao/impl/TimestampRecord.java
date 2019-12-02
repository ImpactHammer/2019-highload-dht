package ru.mail.polis.dao.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class TimestampRecord {
    private byte[] value;
    private long timestamp;

    public static int TYPE_EMPTY;
    public static int TYPE_DELETED = 1;
    public static int TYPE_VALUE = 2;

    private int type;

    /**
     * Constructor.
     *
     * @param value - byte array to be wrapped
     */
    public TimestampRecord(final byte[] value) {
        this(value, System.currentTimeMillis());
    }

    /**
     * Constructor.
     *
     * @param value     - byte array to be wrapped
     * @param timestamp - timestamp of record creation
     */
    public TimestampRecord(final byte[] value, final long timestamp) {
        if (value == null) {
            this.type = TYPE_DELETED;
        } else if (value.length == 0) {
            this.type = TYPE_EMPTY;
        } else {
            this.type = TYPE_VALUE;
        }
        this.value = value == null ? null : value.clone();
        this.timestamp = timestamp;
    }

    public byte[] getValue() {
        return this.value == null ? null : value.clone();
    }

    public int getType() {
        return this.type;
    }

    /**
     * Serialize this record to byte array.
     *
     * @return resulting byte array
     */
    public byte[] toByteArray() throws IOException {
        ByteBuffer buffer;
        if (value == null) {
            buffer = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
            buffer.putInt(this.type);
            buffer.putLong(this.timestamp);
        } else {
            buffer = ByteBuffer.allocate(Long.BYTES + Integer.BYTES + value.length);
            buffer.putInt(this.type);
            buffer.put(value);
            buffer.putLong(this.timestamp);
        }
        return buffer.array();
    }

    /**
     * Deserialize record from byte array.
     *
     * @param bytes - given byte array
     * @return resulting record
     */
    public static TimestampRecord fromByteArray(final byte[] bytes) {
        if (bytes == null || bytes.length < Long.BYTES + Integer.BYTES) {
            return null;
        }
        final ByteBuffer buffer = ByteBuffer.wrap(bytes);
        final int type = buffer.getInt();
        byte[] value;
        if (type == TYPE_VALUE) {
            value = new byte[buffer.remaining() - Long.BYTES];
            buffer.get(value);
        } else if (type == TYPE_EMPTY) {
            value = new byte[0];
        } else if (type == TYPE_DELETED) {
            value = null;
        } else {
            return null;
        }
        final TimestampRecord result = new TimestampRecord(value);
        result.setTimestamp(buffer.getLong());
        return result;
    }

    /**
     * Get record with latest timestamp from list.
     *
     * @param records - list of records
     * @return resulting record
     */
    public static TimestampRecord latestOf(final List<TimestampRecord> records) {
        TimestampRecord result = null;
        for (final TimestampRecord record : records) {
            if (record == null) {
                continue;
            }
            if (result == null || record.timestamp > result.timestamp) {
                result = record;
            }
        }
        return result;
    }

    private void setTimestamp(final long timestamp) {
        this.timestamp = timestamp;
    }
}
