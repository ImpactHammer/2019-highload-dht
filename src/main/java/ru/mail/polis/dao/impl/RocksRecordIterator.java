package ru.mail.polis.dao.impl;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import ru.mail.polis.Record;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class RocksRecordIterator implements Iterator<Record> {
    private final RocksIterator rocksIterator;

    RocksRecordIterator(final RocksDB db, final ByteBuffer from) {
        super();
        rocksIterator = db.newIterator();
        final ByteBuffer from_copy = from.duplicate();
        byte[] bytes = new byte[from_copy.capacity()];
        from_copy.get(bytes, 0, bytes.length);
        rocksIterator.seek(bytes);
    }

    @Override
    public boolean hasNext() {
        return rocksIterator.isValid();
    }

    @Override
    public Record next() {
        final Record resultRecord = Record.of(ByteBuffer.wrap(rocksIterator.key()),
                ByteBuffer.wrap(rocksIterator.value()));
        if (rocksIterator.isValid()) {
            rocksIterator.next();
        }
        return resultRecord;
    }
}
