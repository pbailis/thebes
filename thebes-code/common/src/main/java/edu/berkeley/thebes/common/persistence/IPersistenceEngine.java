package edu.berkeley.thebes.common.persistence;

import edu.berkeley.thebes.common.data.DataItem;

import java.io.IOException;

public interface IPersistenceEngine {
    public void open() throws IOException;

    public boolean put(String key, DataItem value);

    public DataItem get(String key);

    public void close() throws IOException;
}