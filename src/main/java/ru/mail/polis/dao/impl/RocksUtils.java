package ru.mail.polis.dao.impl;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Arrays;

public final class RocksUtils {

    RocksUtils() {

    }

    /**
     * Retrieve array from a {@link java.nio.ByteBuffer}.
     *
     * @param buffer byte buffer to extract from
     * @return array
     */
    public static byte[] toArray(@NotNull final ByteBuffer buffer) {
        final var bufferCopy = buffer.duplicate();
        final var array = new byte[bufferCopy.remaining()];
        bufferCopy.get(array);
        return array;
    }

    /**
     * Retrieve array from a {@link java.nio.ByteBuffer} and shift all bytes by {@link
     * Byte#MIN_VALUE}.
     *
     * @param buffer byte buffer to extract from
     * @return array with all bytes shifted
     */
    static byte[] toArrayShifted(@NotNull final ByteBuffer buffer) {
        final var bufferCopy = buffer.duplicate();
        final var array = new byte[bufferCopy.remaining()];
        bufferCopy.get(array);
        shiftArrayInplace(array, Byte.MIN_VALUE);
        return array;
    }

    /**
     * Wrap byte array into {@link java.nio.ByteBuffer}.
     *
     * @param array byte array to wrap
     * @return ByteBuffer with all bytes shifted back to normal values
     */
    static ByteBuffer fromArrayShifted(@NotNull final byte[] array) {
        final var arrayCopy = Arrays.copyOf(array, array.length);
        shiftArrayInplace(arrayCopy, -Byte.MIN_VALUE);
        return ByteBuffer.wrap(arrayCopy);
    }

    private static void shiftArrayInplace(final byte[] array, final int shift) {
        for (int i = 0; i < array.length; i++) {
            final var uint = Byte.toUnsignedInt(array[i]);
            array[i] = (byte) (uint - shift);
        }
    }
}
