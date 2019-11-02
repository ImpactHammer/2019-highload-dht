package ru.mail.polis.dao.impl;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;

import org.rocksdb.BuiltinComparator;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;

public class RocksDAO implements DAO {
    private final RocksDB db;

    /**
     * Constructor.
     *
     * @param data Database file
     */
    public RocksDAO(@NotNull final File data) throws RocksDBException {
        final Options options = new Options().setCreateIfMissing(true);
        options.setComparator(BuiltinComparator.BYTEWISE_COMPARATOR);
        db = RocksDB.open(options, data.getAbsolutePath());
    }

    @Override
    public void upsert(
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value) throws IOException {
        synchronized (this) {
            try {
                db.put(RocksUtils.toArrayShifted(key), RocksUtils.toArrayShifted(value));
            } catch (RocksDBException e) {
                throw new IOException(e);
            }
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        synchronized (this) {
            try {
                db.delete(RocksUtils.toArrayShifted(key));
            } catch (RocksDBException e) {
                throw new IOException(e);
            }
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        return new RocksRecordIterator(db, from);
    }

    @Override
    public void close() throws IOException {
        db.close();
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull final ByteBuffer key) throws NoSuchElementException, IOException {
        synchronized (this) {
            byte[] bytes = null;
            try {
                bytes = db.get(RocksUtils.toArrayShifted(key));
            } catch (RocksDBException e) {
                throw new IOException(e);
            }
            if (bytes == null) {
                throw new NoSuchElementLite();
            }
            return RocksUtils.fromArrayShifted(bytes);
        }
    }

    @Override
    public void compact() throws IOException {
        try {
            db.compactRange();
        } catch (RocksDBException e) {
            throw new IOException(e);
        }
    }

    /**
     * Get record from DB.
     *
     * @param keys to define key
     * @return record
     */
    @NotNull
    public TimestampRecord getRecordWithTimestamp(@NotNull final ByteBuffer keys)
            throws IOException, NoSuchElementException {
        try {
            final byte[] packedKey = RocksUtils.toArrayShifted(keys);
            final byte[] valueByteArray = db.get(packedKey);
            return TimestampRecord.fromBytes(valueByteArray);
        } catch (RocksDBException exception) {
            throw new NoSuchElementLite();
        }
    }

    /**
     * Put record into DB.
     *
     * @param keys to define key
     * @param values to define value
     */
    public void upsertRecordWithTimestamp(@NotNull final ByteBuffer keys,
                                          @NotNull final ByteBuffer values) throws IOException {
        try {
            final var record = TimestampRecord.fromValue(values, System.currentTimeMillis());
            final byte[] packedKey = RocksUtils.toArrayShifted(keys);
            final byte[] arrayValue = record.toBytes();
            db.put(packedKey, arrayValue);
        } catch (RocksDBException e) {
            throw new NoSuchElementLite();
        }
    }

    /**
     * Delete record from DB.
     *
     * @param key to define key
     */
    public void removeRecordWithTimestamp(@NotNull final ByteBuffer key) throws IOException {
        try {
            final byte[] packedKey = RocksUtils.toArrayShifted(key);
            final var record = TimestampRecord.tombstone(System.currentTimeMillis());
            final byte[] arrayValue = record.toBytes();
            db.put(packedKey, arrayValue);
        } catch (RocksDBException e) {
            throw new NoSuchElementLite();
        }
    }
}

