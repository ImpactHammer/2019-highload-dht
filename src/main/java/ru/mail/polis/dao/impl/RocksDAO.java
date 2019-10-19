package ru.mail.polis.dao.impl;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import org.rocksdb.*;
import org.rocksdb.util.BytewiseComparator;

import ru.mail.polis.Record;
import ru.mail.polis.dao.DAO;
import ru.mail.polis.dao.Iters;

public class RocksDAO implements DAO {
    private RocksDB db;

    public RocksDAO(@NotNull final File data) {
        RocksDB.loadLibrary();
        final Options options = new Options().setCreateIfMissing(true);
        options.setComparator(new BytewiseComparator(new ComparatorOptions()));
        try {
            db = RocksDB.open(options, data.getAbsolutePath());
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    private ByteBuffer deepCopy(ByteBuffer src) {
        ByteBuffer clone = ByteBuffer.allocate(src.capacity());
        src.rewind();
        clone.put(src);
        src.rewind();
        clone.flip();
        return clone;
    }

    public void upsert(
            @NotNull ByteBuffer key,
            @NotNull ByteBuffer value) {
        try {
            db.put(deepCopy(key).array(), deepCopy(value).array());
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    public void remove(@NotNull ByteBuffer key) {
        try {
            db.delete(deepCopy(key).array());
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull ByteBuffer from) {
        return new RocksRecordIterator(db, from);
    }

    @Override
    public void close() throws IOException {
        db.close();
    }

    @NotNull
    @Override
    public ByteBuffer get(@NotNull ByteBuffer key) throws NoSuchElementException {
        byte[] bytes = null;
        try {
            bytes = db.get(deepCopy(key).array());
        } catch (RocksDBException e) {
            e.printStackTrace();
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
        } catch (RocksDBException e) {
            e.printStackTrace();
        }
    }
}
