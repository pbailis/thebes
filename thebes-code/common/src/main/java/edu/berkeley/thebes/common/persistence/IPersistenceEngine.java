package edu.berkeley.thebes.common.persistence;

import edu.berkeley.thebes.common.data.DataItem;
import org.apache.thrift.TException;

import java.io.IOException;

public interface IPersistenceEngine {
    public void open() throws IOException;

    public boolean put(String key, DataItem value) throws TException;

    public DataItem get(String key) throws TException;

    public void delete(String key) throws TException;

    public void close() throws IOException;
}