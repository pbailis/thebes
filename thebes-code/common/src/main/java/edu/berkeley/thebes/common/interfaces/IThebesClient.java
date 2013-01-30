package edu.berkeley.thebes.common.interfaces;

import java.nio.ByteBuffer;

public interface IThebesClient {
    public void beginTransaction();
    public boolean endTransaction();
    public boolean put(String key, ByteBuffer value);
    public ByteBuffer get(String key);
}