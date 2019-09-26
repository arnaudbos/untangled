package io.monkeypatch.untangled.utils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

/**
 * Input stream that generates random data with three possible configurations:
 * <ul>
 *     <li>Fully random stream created by calling one argument constructor.</li>
 *     <li>Stream that starts with fixed uniform block of bytes followed by fully random data.</li>
 *     <li>Stream that creates fixed blocks and populates each with randomly generated new byte.</li>
 * </ul>
 *
 * The first option is quite universal and may be used for any type of tests. The second option might be
 * useful when testing logic dependent on compressed data size and archived data are transferred via
 * network. However, the second configuration is not efficient in terms of complexity of compression and
 * its speed. In order to remove this bias the third configuration was introduced to provide evenly
 * distributed data that provide steady load for compression and decompression of generated files.
 *
 * @author Petr Fiala, Jakub Stas
 */
public class RandomGeneratedInputStream extends InputStream {

    private final Random random = new Random();

    /** Type of randomization strategy. */
    private final Type type;

    /** Target size of the stream. */
    private final long size;

    /** Size of the block populated by same byte. */
    private final long blockSize;

    /** Size of expandable block being populated by same byte. */
    private long currentBlockSize;

    /** Value of last generated byte. */
    private int lastUsedByte;

    /** Internal counter. */
    private long index;

    /**
     * @param size target size of the stream [byte]
     */
    public RandomGeneratedInputStream(long size) {
        this(size, 1, Type.FIXED);
    }

    /**
     * @param size target size of the stream [byte]
     * @param blockSize size of the block populated by same byte [byte]
     * @param type randomization strategy
     */
    public RandomGeneratedInputStream(long size, long blockSize, Type type) {
        super();

        if (blockSize < 1) {
            throw new IllegalArgumentException("Block size must be at least one byte!");
        }

        this.size = size;
        this.type = type;
        this.blockSize = blockSize;
        this.currentBlockSize = blockSize;
        this.lastUsedByte = random.nextInt(255);
    }

    @Override
    public int read() throws IOException {
        if (index == size) {
            return -1;
        }

        switch (type) {
            case ITERATIVE:
                if (index == currentBlockSize) {
                    lastUsedByte = random.nextInt(255);
                    currentBlockSize += blockSize;
                }
                break;
            case FIXED:
                if (index >= blockSize) {
                    lastUsedByte = random.nextInt(255);
                }
                break;
            default:
                break;
        }

        index++;

        return lastUsedByte;
    }

    /**
     * Type of randomization strategy used to populate the stream.
     */
    public static enum Type {
        ITERATIVE,
        FIXED;
    }
}