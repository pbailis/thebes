package edu.berkeley.thebes.server.persistence;

import java.nio.ByteBuffer;

public interface IPersistenceEngine {
    public void open();

    public boolean put(String key, ByteBuffer value);

    public ByteBuffer get(String key);

    public void close();
}