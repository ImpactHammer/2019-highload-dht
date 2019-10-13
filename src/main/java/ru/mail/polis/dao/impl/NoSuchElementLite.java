package ru.mail.polis.dao.impl;

import java.util.NoSuchElementException;

public class NoSuchElementLite extends NoSuchElementException {
    @Override
    public synchronized Throwable fillInStackTrace() {
        return this;
    }
}
