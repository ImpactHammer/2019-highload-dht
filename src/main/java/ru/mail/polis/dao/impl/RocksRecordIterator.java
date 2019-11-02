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
        final byte[] bytes = RocksUtils.toArrayShifted(from);

        rocksIterator.seek(bytes);
    }

    @Override
    public boolean hasNext() {
        return rocksIterator.isValid();
    }

    @Override
    public Record next() {
        final Record resultRecord = Record.of(RocksUtils.fromArrayShifted(rocksIterator.key()),
                RocksUtils.fromArrayShifted(rocksIterator.value()));
        if (rocksIterator.isValid()) {
            rocksIterator.next();
        }
        return resultRecord;
    }
}
