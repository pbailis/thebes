package edu.berkeley.thebes.common.persistence;

import edu.berkeley.thebes.common.data.DataItem;

public interface IPersistenceEngine {
    public void open();

    public boolean put(String key, DataItem value);

    public DataItem get(String key);

    public void close();
}