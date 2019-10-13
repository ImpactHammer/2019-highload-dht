package ru.mail.polis.dao.impl;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksIterator;
import ru.mail.polis.Record;

import java.nio.ByteBuffer;
import java.util.Iterator;

public class RocksRecordIterator implements Iterator<Record> {
    private RocksIterator rocksIterator;

    RocksRecordIterator(RocksDB db, ByteBuffer from) {
        super();
        rocksIterator = db.newIterator();
        rocksIterator.seek(from.array());
    }

    @Override
    public boolean hasNext() {
        return rocksIterator.isValid();
    }

    @Override
    public Record next() {
        Record resultRecord = Record.of(ByteBuffer.wrap(rocksIterator.key()),
                ByteBuffer.wrap(rocksIterator.value()));
        if (rocksIterator.isValid()) {
            rocksIterator.next();
        }
        return resultRecord;
    }
}
