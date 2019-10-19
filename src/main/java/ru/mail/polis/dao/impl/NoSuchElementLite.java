package ru.mail.polis.dao.impl;

import java.util.NoSuchElementException;

public class NoSuchElementLite extends NoSuchElementException {
    private static final long serialVersionUID = 6769829250639411880L;

    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
