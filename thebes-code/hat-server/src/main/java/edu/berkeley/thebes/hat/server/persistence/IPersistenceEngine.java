package edu.berkeley.thebes.hat.server.persistence;

import java.nio.ByteBuffer;

import edu.berkeley.thebes.common.thrift.DataItem;

public interface IPersistenceEngine {
    public void open();

    public boolean put(String key, DataItem value);

    public DataItem get(String key);

    public void close();
}