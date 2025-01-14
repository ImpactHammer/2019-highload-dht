package ru.mail.polis.dao.impl;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;

import org.rocksdb.ComparatorOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.util.BytewiseComparator;

import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;

public class RocksDAO implements DAO {
    private RocksDB db;

    /**
     * @param data Database file
     */
    public RocksDAO(@NotNull final File data) throws RocksDBException {
        RocksDB.loadLibrary();
        final Options options = new Options().setCreateIfMissing(true);
        options.setComparator(new BytewiseComparator(new ComparatorOptions()));
        db = RocksDB.open(options, data.getAbsolutePath());
    }

    private ByteBuffer deepCopy(final ByteBuffer src) {
        final ByteBuffer srcCopy = src.duplicate();
        final ByteBuffer clone = ByteBuffer.allocate(srcCopy.capacity());
        srcCopy.rewind();
        clone.put(srcCopy);
        clone.flip();
        return clone;
    }

    @Override
    public void upsert(
            @NotNull final ByteBuffer key,
            @NotNull final ByteBuffer value) {
        try {
            db.put(deepCopy(key).array(), deepCopy(value).array());
        } catch (RocksDBException ignored) {

        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        try {
            db.delete(deepCopy(key).array());
        } catch (RocksDBException ignored) {

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
    public ByteBuffer get(@NotNull final ByteBuffer key) throws NoSuchElementException {
        byte[] bytes = null;
        try {
            bytes = db.get(deepCopy(key).array());
        } catch (RocksDBException ignored) {

        }
        if (bytes == null) {
            throw new NoSuchElementLite();
        }
        return ByteBuffer.wrap(bytes);
    }

    @Override
    public void compact() {
        try {
            db.compactRange();
        } catch (RocksDBException ignored) {

        }
    }
}
