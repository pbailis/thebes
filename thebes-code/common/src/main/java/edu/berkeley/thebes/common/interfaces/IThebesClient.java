package edu.berkeley.thebes.common.interfaces;

import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import javax.naming.ConfigurationException;
import java.io.FileNotFoundException;
import java.nio.ByteBuffer;

public interface IThebesClient {
    public void open() throws TTransportException, ConfigurationException, FileNotFoundException;

    public void beginTransaction() throws TException;
    public boolean endTransaction() throws TException;

    public boolean put(String key, ByteBuffer value) throws TException;
    public ByteBuffer get(String key) throws TException;

    public void close();
}