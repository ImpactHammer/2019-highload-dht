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
}
